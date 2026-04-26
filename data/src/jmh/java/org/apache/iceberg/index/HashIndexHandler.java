/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.index;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.util.Lists;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple hash-bucketed inverted-index file format.
 *
 * <p>The number of buckets is fixed at construction time. Every key is mapped to a bucket via a
 * 32-bit hash + Lemire fastrange. The writer measures the heaviest bucket and pads every bucket on
 * disk to that size, so the file layout is a flat {@code numBuckets * maxBucketSize * entrySize}
 * byte rectangle that can be indexed directly without any per-bucket offset table or count array.
 *
 * <p>Empty slots are marked by {@code pos == Long.MIN_VALUE}; lookup scans the {@code
 * maxBucketSize} entries of the target bucket and stops at the first empty slot or first byte-equal
 * key. The schema-driven key encoder is self-delimiting (every variable-length field is
 * length-prefixed), so the padded zero tail in each slot can never collide with a real key, which
 * lets us drop the per-entry {@code keyLength} field entirely and compare keys via {@link
 * Arrays#mismatch} over the full {@code maxKeyLength} slot.
 *
 * <p><b>File layout</b> (all integers big-endian):
 *
 * <pre>
 *   [0..5]   MAGIC = "HIDX01"
 *   [6..9]   int32  formatVersion
 *   [10..13] int32  numBuckets
 *   [14..17] int32  maxBucketSize       (entries per bucket, after padding)
 *   [18..21] int32  prefixLength
 *   [22..25] int32  maxKeyLength
 *   [26..29] int32  maxSuffixLength
 *   [30..]   prefix bytes               (prefixLength bytes)
 *   [..EOF]  data block                 (numBuckets * maxBucketSize * entrySize bytes)
 * </pre>
 *
 * <p><b>Per-entry layout</b> (total {@code entrySize = 2 + 8 + maxKeyLength + maxSuffixLength};
 * {@code pos == Long.MIN_VALUE} marks an empty slot):
 *
 * <pre>
 *   int16  suffixLength
 *   int64  pos                          (Long.MIN_VALUE marks an empty slot)
 *   bytes  key    (padded to maxKeyLength with zeros)
 *   bytes  suffix (padded to maxSuffixLength with zeros)
 * </pre>
 */
public class HashIndexHandler implements IndexHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HashIndexHandler.class);

  private static final byte[] MAGIC = "HIDX01".getBytes(StandardCharsets.US_ASCII);
  private static final int FORMAT_VERSION = 1;

  /** {@code int16 suffixLength + int64 pos}; the key length is implicit (slot is padded). */
  private static final int ENTRY_HEADER_BYTES = 2 + 8;

  /** Reserved {@code pos} value used to mark an empty slot. {@code add()} rejects it. */
  private static final long EMPTY_POS = Long.MIN_VALUE;

  /** Fixed-size header up to (but not including) the prefix bytes. */
  private static final int HEADER_FIXED_LENGTH = 6 + 4 + 4 + 4 + 4 + 4 + 4;

  /**
   * Inputs to {@link #recommendedReadBlockSize(long, int)}. Calibrated so that for typical
   * workloads (LONG / UUID / short STRING keys with a shared path prefix) the estimated per-bucket
   * size is close to the actual {@code bucketBytes} the {@link Reader} computes from the on-disk
   * header, keeping the under/over-shoot warnings silent.
   *
   * <p>{@code BUCKET_LOAD_FACTOR}: multiplier applied to the average bucket occupancy to model
   * writer-side hash collisions. {@code ESTIMATED_ENTRY_BYTES}: conservative per-entry on-disk size
   * in bytes. {@code MIN_RECOMMENDED_BLOCK_BYTES}: lower bound; also covers the open-time header
   * prefetch. {@code MAX_RECOMMENDED_BLOCK_BYTES}: upper bound to keep outlier configs in check.
   */
  private static final double BUCKET_LOAD_FACTOR = 1.2;

  private static final long ESTIMATED_ENTRY_BYTES = 40L;

  private static final int MIN_RECOMMENDED_BLOCK_BYTES = 4096;

  private static final long MAX_RECOMMENDED_BLOCK_BYTES = 64L * 1024 * 1024;

  private static final VarHandle SHORT_BE =
      MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);

  private static final VarHandle LONG_BE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

  private final Function<Record, byte[]> keyEncoder;
  private final long expectedKeyCount;
  private final int numBuckets;

  /**
   * @param schema key schema (same supported types as {@link
   *     MinimalPerfectHashFunctionIndexHandler})
   * @param expectedKeyCount sizing hint for writer / reader buffers; must be {@code > 0}
   * @param numBuckets fixed number of hash buckets; must be {@code > 0}
   */
  public HashIndexHandler(Schema schema, long expectedKeyCount, int numBuckets) {
    if (expectedKeyCount <= 0L) {
      throw new IllegalArgumentException("expectedKeyCount must be > 0: " + expectedKeyCount);
    }

    if (numBuckets <= 0) {
      throw new IllegalArgumentException("numBuckets must be > 0: " + numBuckets);
    }

    this.keyEncoder = MinimalPerfectHashFunctionIndexHandler.keyEncoder(schema);
    this.expectedKeyCount = expectedKeyCount;
    this.numBuckets = numBuckets;
  }

  @Override
  public IndexHandler.Writer writer(OutputFile output) {
    return new Writer(output, keyEncoder, expectedKeyCount, numBuckets);
  }

  @Override
  public IndexHandler.Reader reader(InputFile input) throws IOException {
    return new Reader(input, keyEncoder, expectedKeyCount);
  }

  /**
   * Estimates the largest bounded read this handler's {@link Reader} will issue (a single bucket)
   * and rounds it up to the next power of two so the storage adapter can serve it in one wire GET.
   * {@link #MIN_RECOMMENDED_BLOCK_BYTES} sets a hard floor (also covering the open-time header
   * prefetch) and {@link #MAX_RECOMMENDED_BLOCK_BYTES} caps outlier configurations.
   *
   * <p>The estimate uses {@link #BUCKET_LOAD_FACTOR} times the average bucket occupancy ({@code
   * expectedKeyCount / numBuckets}) to approximate the writer's hash collisions, and {@link
   * #ESTIMATED_ENTRY_BYTES} for the per-entry header + key + path-suffix slot. This is
   * intentionally conservative: under-estimating would fragment the per-lookup GET into multiple
   * round-trips (very expensive on cloud storage), while over-estimating only wastes a few KB of
   * wire bandwidth on the rare end-of-bucket fetch.
   */
  @Override
  public Integer recommendedReadBlockSize() {
    return recommendedReadBlockSize(expectedKeyCount, numBuckets);
  }

  /** Pure helper so {@link Reader} can self-check the estimate without an enclosing instance. */
  private static int recommendedReadBlockSize(long expectedKeyCount, int numBuckets) {
    long avgBucket = Math.max(1L, expectedKeyCount / numBuckets);
    long estimatedBucketBytes =
        Math.max(
            MIN_RECOMMENDED_BLOCK_BYTES,
            (long) Math.ceil(avgBucket * BUCKET_LOAD_FACTOR) * ESTIMATED_ENTRY_BYTES);
    long capped = Math.min(estimatedBucketBytes, MAX_RECOMMENDED_BLOCK_BYTES);
    int blockSize = Integer.highestOneBit(Math.toIntExact(capped - 1)) << 1;
    return Math.max(blockSize, MIN_RECOMMENDED_BLOCK_BYTES);
  }

  // --------------------------------------------------------------------------
  // Hashing
  // --------------------------------------------------------------------------

  /**
   * Maps {@code key} to {@code [0, numBuckets)} via MurmurHash3 (Guava's {@link
   * Hashing#murmur3_32_fixed()}, the same hash Iceberg uses for the {@code bucket} partition
   * transform) and Lemire fastrange (one 64-bit multiply + shift, no modulo).
   */
  static int bucketOf(byte[] key, int numBuckets) {
    long h = Hashing.murmur3_32_fixed().hashBytes(key).asInt() & 0xFFFFFFFFL;
    return (int) ((h * numBuckets) >>> 32);
  }

  // --------------------------------------------------------------------------
  // Writer
  // --------------------------------------------------------------------------

  private static class Writer implements IndexHandler.Writer {
    private final OutputFile output;
    private final Function<Record, byte[]> keyEncoder;
    private final int numBuckets;
    private final List<byte[]> keys;
    private final List<String> filePaths;
    private final LongArrayList positions;
    private boolean closed;

    Writer(
        OutputFile output,
        Function<Record, byte[]> keyEncoder,
        long expectedKeyCount,
        int numBuckets) {
      this.output = output;
      this.keyEncoder = keyEncoder;
      this.numBuckets = numBuckets;
      int initialCapacity = (int) Math.min(Integer.MAX_VALUE - 16L, expectedKeyCount);
      this.keys = Lists.newArrayListWithCapacity(initialCapacity);
      this.filePaths = Lists.newArrayListWithCapacity(initialCapacity);
      this.positions = new LongArrayList(initialCapacity);
    }

    @Override
    public void add(Record key, String filePath, long pos) {
      if (closed) {
        throw new IllegalStateException("Writer already closed");
      }

      if (pos == EMPTY_POS) {
        throw new IllegalArgumentException(
            "pos == Long.MIN_VALUE is reserved as the empty-slot sentinel");
      }

      byte[] encoded = keyEncoder.apply(key);
      if (encoded.length == 0) {
        throw new IllegalArgumentException("Encoded key cannot be empty");
      }

      keys.add(encoded);
      filePaths.add(filePath);
      positions.add(pos);
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }

      closed = true;

      int n = keys.size();

      // 1) Pre-compute every entry's bucket and find the heaviest bucket.
      int[] buckets = new int[n];
      int[] counts = new int[numBuckets];
      int maxBucketSize = 0;
      int nonEmptyBuckets = 0;
      for (int i = 0; i < n; i++) {
        int b = bucketOf(keys.get(i), numBuckets);
        buckets[i] = b;
        int c = ++counts[b];
        if (c == 1) {
          nonEmptyBuckets++;
        }

        if (c > maxBucketSize) {
          maxBucketSize = c;
        }
      }

      // 2) Encode paths once, compute longest common prefix.
      byte[][] pathBytes = new byte[n][];
      for (int i = 0; i < n; i++) {
        pathBytes[i] = filePaths.get(i).getBytes(StandardCharsets.UTF_8);
        filePaths.set(i, null);
      }

      filePaths.clear();

      byte[] prefix = longestCommonPrefix(pathBytes);

      int maxSuffixLength = 0;
      for (byte[] path : pathBytes) {
        int suffixLength = path.length - prefix.length;
        if (suffixLength > maxSuffixLength) {
          maxSuffixLength = suffixLength;
        }
      }

      if (maxSuffixLength > 0xFFFF) {
        throw new IOException(
            "Path suffix " + maxSuffixLength + " bytes exceeds uint16 limit (65535)");
      }

      int maxKeyLength = 0;
      for (byte[] key : keys) {
        if (key.length > maxKeyLength) {
          maxKeyLength = key.length;
        }
      }

      if (maxKeyLength > 0xFFFF) {
        throw new IOException("Key " + maxKeyLength + " bytes exceeds uint16 limit (65535)");
      }

      int entrySize = ENTRY_HEADER_BYTES + maxKeyLength + maxSuffixLength;
      long bucketBytes = (long) maxBucketSize * entrySize;
      long dataLen = (long) numBuckets * bucketBytes;
      byte[] dataBlock = new byte[Math.toIntExact(dataLen)];
      ByteBuffer buf = ByteBuffer.wrap(dataBlock).order(ByteOrder.BIG_ENDIAN);

      // Pre-seed every slot's `pos` field with the empty-slot sentinel. Non-pos bytes stay zero;
      // real entries below overwrite both fields.
      long totalSlots = (long) numBuckets * maxBucketSize;
      for (long s = 0; s < totalSlots; s++) {
        int off = Math.toIntExact(s * entrySize);
        buf.putLong(off + 2, EMPTY_POS);
      }

      // 3) Fill each entry into the next free slot of its bucket. Empty slots keep their
      //    pre-seeded `pos == EMPTY_POS` marker.
      int[] fillCursor = new int[numBuckets];
      for (int i = 0; i < n; i++) {
        byte[] key = keys.get(i);
        int b = buckets[i];
        int slot = fillCursor[b]++;
        long bucketOffset = (long) b * bucketBytes;
        int off = Math.toIntExact(bucketOffset + (long) slot * entrySize);
        byte[] path = pathBytes[i];
        int suffixLength = path.length - prefix.length;
        buf.putShort(off, (short) suffixLength);
        buf.putLong(off + 2, positions.getLong(i));
        System.arraycopy(key, 0, dataBlock, off + ENTRY_HEADER_BYTES, key.length);
        System.arraycopy(
            path, prefix.length, dataBlock, off + ENTRY_HEADER_BYTES + maxKeyLength, suffixLength);

        keys.set(i, null);
        pathBytes[i] = null;
      }

      keys.clear();
      positions.clear();

      // 4) Space-efficiency report. Useful entries / total padded entries; also flags the
      //    bucket-skew penalty (non-empty buckets vs. numBuckets).
      logSpaceEfficiency(n, maxBucketSize, nonEmptyBuckets, entrySize, dataLen);

      try (OutputStream os = output.create();
          DataOutputStream dos = new DataOutputStream(os)) {
        dos.write(MAGIC);
        dos.writeInt(FORMAT_VERSION);
        dos.writeInt(numBuckets);
        dos.writeInt(maxBucketSize);
        dos.writeInt(prefix.length);
        dos.writeInt(maxKeyLength);
        dos.writeInt(maxSuffixLength);
        dos.write(prefix);
        dos.write(dataBlock);
        dos.flush();
      }
    }

    private void logSpaceEfficiency(
        int n, int maxBucketSize, int nonEmptyBuckets, int entrySize, long dataLen) {
      long totalSlots = (long) numBuckets * maxBucketSize;
      double slotUtilization = totalSlots == 0 ? 0.0 : (double) n / (double) totalSlots;
      double bucketUtilization =
          numBuckets == 0 ? 0.0 : (double) nonEmptyBuckets / (double) numBuckets;
      double avgLoad = numBuckets == 0 ? 0.0 : (double) n / (double) numBuckets;
      double skew = avgLoad == 0.0 ? Double.NaN : (double) maxBucketSize / avgLoad;
      long usefulBytes = (long) n * entrySize;
      long wastedBytes = dataLen - usefulBytes;

      LOG.info(
          "HashIndexHandler space report: keys={}, numBuckets={}, nonEmptyBuckets={} ({}%), "
              + "avgLoad={}, maxBucketSize={} (skew x{}), entrySize={} B, "
              + "dataBlock={} B, useful={} B, wasted={} B, slotUtilization={}%",
          n,
          numBuckets,
          nonEmptyBuckets,
          String.format(java.util.Locale.ROOT, "%.2f", bucketUtilization * 100.0),
          String.format(java.util.Locale.ROOT, "%.2f", avgLoad),
          maxBucketSize,
          String.format(java.util.Locale.ROOT, "%.2f", skew),
          entrySize,
          dataLen,
          usefulBytes,
          wastedBytes,
          String.format(java.util.Locale.ROOT, "%.2f", slotUtilization * 100.0));
    }
  }

  // --------------------------------------------------------------------------
  // Reader
  // --------------------------------------------------------------------------

  private static class Reader implements IndexHandler.Reader {
    private final SeekableInputStream stream;
    private final boolean rangeReadable;
    private final Function<Record, byte[]> keyEncoder;
    private final int numBuckets;
    private final int maxBucketSize;
    private final int maxKeyLength;
    private final int entrySize;
    private final long bucketBytes;
    private final long dataBlockOffset;
    private final int suffixOffset;
    private final String prefixStr;
    private final boolean prefixEmpty;

    /** Per-Reader scratch buffer reused by every {@link #lookup(Record)} call. */
    private final byte[] bucketBuf;

    /**
     * Per-Reader scratch holding the lookup key padded with zeros to {@link #maxKeyLength}, so a
     * single {@link Arrays#equals} call can compare the full padded key slot in one shot.
     */
    private final byte[] paddedKey;

    Reader(InputFile input, Function<Record, byte[]> keyEncoder, long expectedKeyCount)
        throws IOException {
      this.stream = input.newStream();
      this.keyEncoder = keyEncoder;
      this.rangeReadable = stream instanceof RangeReadable;

      // 1) Speculative bulk read covering the fixed header + (most of) the prefix bytes. The
      //    exact prefix length is recorded in the header, but in practice it's a single path
      //    prefix that comfortably fits in a few KB. Issuing one large read up front, rather
      //    than 7 tiny reads via DataInputStream, collapses Reader-open into a single
      //    underlying read() on object stores.
      long fileLength;
      try {
        fileLength = input.getLength();
      } catch (RuntimeException e) {
        fileLength = Long.MAX_VALUE;
      }

      // 4 KB slack covers the longest-common path prefix in virtually all real workloads.
      int prefetch =
          (int)
              Math.min(
                  (long) HEADER_FIXED_LENGTH + 4096L,
                  Math.min(Integer.MAX_VALUE - 16L, fileLength));
      if (prefetch < HEADER_FIXED_LENGTH) {
        throw new IOException("File too short to contain HashIndexHandler header: " + fileLength);
      }

      // Bounded positional read. RangeReadable.readFully passes an explicit (start, end) to
      // the storage adapter so e.g. ADLS issues exactly one GET sized to `prefetch`, instead
      // of pulling a blockSize-aligned chunk via the buffered stream.
      byte[] prefetched = new byte[prefetch];
      readRange(0L, prefetched, 0, prefetch);

      // Parse fixed header out of the prefetched buffer.
      ByteBuffer hd = ByteBuffer.wrap(prefetched).order(ByteOrder.BIG_ENDIAN);
      byte[] magic = new byte[MAGIC.length];
      hd.get(magic);
      if (!Arrays.equals(magic, MAGIC)) {
        throw new IOException("Not a HashIndexHandler file (bad magic)");
      }

      int formatVersion = hd.getInt();
      if (formatVersion != FORMAT_VERSION) {
        throw new IOException("Unsupported HashIndexHandler file version: " + formatVersion);
      }

      this.numBuckets = hd.getInt();
      this.maxBucketSize = hd.getInt();
      int prefixLength = hd.getInt();
      this.maxKeyLength = hd.getInt();
      int maxSuffixLength = hd.getInt();

      // 2) Materialise the prefix bytes. In the common case they already sit inside
      //    `prefetched`; otherwise read just the missing tail.
      int prefetchedPrefix = prefetched.length - HEADER_FIXED_LENGTH;
      byte[] prefix = new byte[prefixLength];
      if (prefetchedPrefix >= prefixLength) {
        System.arraycopy(prefetched, HEADER_FIXED_LENGTH, prefix, 0, prefixLength);
      } else {
        System.arraycopy(prefetched, HEADER_FIXED_LENGTH, prefix, 0, prefetchedPrefix);
        int missing = prefixLength - prefetchedPrefix;
        readRange((long) HEADER_FIXED_LENGTH + prefetchedPrefix, prefix, prefetchedPrefix, missing);
      }

      this.entrySize = ENTRY_HEADER_BYTES + maxKeyLength + maxSuffixLength;
      this.bucketBytes = (long) maxBucketSize * entrySize;
      this.dataBlockOffset = (long) HEADER_FIXED_LENGTH + prefixLength;
      this.suffixOffset = ENTRY_HEADER_BYTES + maxKeyLength;
      this.prefixStr = new String(prefix, StandardCharsets.UTF_8);
      this.prefixEmpty = prefixStr.isEmpty();
      this.bucketBuf = new byte[Math.toIntExact(bucketBytes)];
      this.paddedKey = new byte[maxKeyLength];

      // Sanity-check the recommendedReadBlockSize() estimate against the actual bucket size,
      // so handler tuning shows up at runtime instead of silently misconfiguring the storage
      // adapter. Under-shoot fragments every per-lookup readFully into multiple wire GETs (very
      // expensive on cloud storage); significant over-shoot wastes bandwidth on each GET. Both
      // are logged at WARN so a benchmark / catalog operator can tighten the estimate without
      // digging through HTTP traces.
      int recommended = recommendedReadBlockSize(expectedKeyCount, numBuckets);
      if (bucketBytes > recommended) {
        LOG.warn(
            "HashIndexHandler Reader bucket exceeds recommendedReadBlockSize: bucketBytes={} B"
                + " > recommended={} B (numBuckets={}, maxBucketSize={}, entrySize={},"
                + " expectedKeyCount={}). Per-lookup GETs will fragment; consider raising the"
                + " per-entry size estimate or lowering numBuckets.",
            bucketBytes,
            recommended,
            numBuckets,
            maxBucketSize,
            entrySize,
            expectedKeyCount);
      } else if (bucketBytes * 2 < recommended) {
        LOG.warn(
            "HashIndexHandler Reader bucket far below recommendedReadBlockSize: bucketBytes={} B"
                + " vs recommended={} B (>2x; numBuckets={}, maxBucketSize={}, entrySize={},"
                + " expectedKeyCount={}). Wire bandwidth wasted; consider lowering the per-entry"
                + " size estimate or raising numBuckets.",
            bucketBytes,
            recommended,
            numBuckets,
            maxBucketSize,
            entrySize,
            expectedKeyCount);
      }
    }

    /** Bounded positional read that prefers a single tight GET via RangeReadable. */
    private void readRange(long position, byte[] buffer, int offset, int length)
        throws IOException {
      if (rangeReadable) {
        ((RangeReadable) stream).readFully(position, buffer, offset, length);
      } else {
        stream.seek(position);
        readFully(stream, buffer, offset, length);
      }
    }

    @Override
    public IndexHandler.Hit lookup(Record key) throws IOException {
      byte[] encoded = keyEncoder.apply(key);
      if (encoded.length > maxKeyLength) {
        return null;
      }

      // Pad the encoded key (with zeros in the trailing bytes) so a single Arrays.equals over
      // the full maxKeyLength slot suffices -- no per-slot length probe needed.
      System.arraycopy(encoded, 0, paddedKey, 0, encoded.length);
      if (encoded.length < maxKeyLength) {
        Arrays.fill(paddedKey, encoded.length, maxKeyLength, (byte) 0);
      }

      int bucket = bucketOf(encoded, numBuckets);
      long bucketOffset = dataBlockOffset + (long) bucket * bucketBytes;
      readRange(bucketOffset, bucketBuf, 0, bucketBuf.length);

      for (int i = 0; i < maxBucketSize; i++) {
        int off = i * entrySize;
        long pos = (long) LONG_BE.get(bucketBuf, off + 2);
        if (pos == EMPTY_POS) {
          // Empty slot terminates the bucket.
          return null;
        }

        if (!Arrays.equals(
            bucketBuf,
            off + ENTRY_HEADER_BYTES,
            off + ENTRY_HEADER_BYTES + maxKeyLength,
            paddedKey,
            0,
            maxKeyLength)) {
          continue;
        }

        int suffixLength = ((short) SHORT_BE.get(bucketBuf, off)) & 0xFFFF;

        String filePath;
        if (prefixEmpty) {
          filePath =
              new String(bucketBuf, off + suffixOffset, suffixLength, StandardCharsets.UTF_8);
        } else {
          filePath =
              prefixStr
                  + new String(bucketBuf, off + suffixOffset, suffixLength, StandardCharsets.UTF_8);
        }

        return new HitImpl(filePath, pos);
      }

      return null;
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }

  // --------------------------------------------------------------------------
  // helpers
  // --------------------------------------------------------------------------

  private static void readFully(InputStream in, byte[] buf, int offset, int len)
      throws IOException {
    int read = 0;
    while (read < len) {
      int n = in.read(buf, offset + read, len - read);
      if (n < 0) {
        throw new EOFException("Unexpected EOF after " + read + " of " + len + " bytes");
      }

      read += n;
    }
  }

  private static byte[] longestCommonPrefix(byte[][] paths) {
    if (paths.length == 0) {
      return new byte[0];
    }

    byte[] first = paths[0];
    int prefixLength = first.length;
    for (int i = 1; i < paths.length && prefixLength > 0; i++) {
      byte[] cur = paths[i];
      int max = Math.min(prefixLength, cur.length);
      int j = 0;
      while (j < max && first[j] == cur[j]) {
        j++;
      }

      prefixLength = j;
    }

    byte[] out = new byte[prefixLength];
    System.arraycopy(first, 0, out, 0, prefixLength);
    return out;
  }
}
