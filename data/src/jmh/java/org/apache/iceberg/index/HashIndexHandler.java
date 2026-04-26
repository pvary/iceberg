/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.iceberg.index;

import java.io.DataInputStream;
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
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
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
    return new Reader(input, keyEncoder);
  }

  // --------------------------------------------------------------------------
  // Hashing
  // --------------------------------------------------------------------------

  /** MurmurHash3-style finalizer over {@link Arrays#hashCode(byte[])}. */
  static int hash(byte[] key) {
    int h = Arrays.hashCode(key);
    h ^= (h >>> 16);
    h *= 0x85ebca6b;
    h ^= (h >>> 13);
    h *= 0xc2b2ae35;
    h ^= (h >>> 16);
    return h;
  }

  /** Lemire fastrange: maps a 32-bit hash to {@code [0, numBuckets)} without modulo. */
  static int bucketOf(byte[] key, int numBuckets) {
    long h = hash(key) & 0xFFFFFFFFL;
    return (int) ((h * numBuckets) >>> 32);
  }

  // --------------------------------------------------------------------------
  // Writer
  // --------------------------------------------------------------------------

  private static class Writer implements IndexHandler.Writer {
    private final OutputFile output;
    private final Function<Record, byte[]> keyEncoder;
    private final int numBuckets;
    private final java.util.ArrayList<byte[]> keys;
    private final java.util.ArrayList<String> filePaths;
    private final it.unimi.dsi.fastutil.longs.LongArrayList positions;
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
      this.keys = new java.util.ArrayList<>(initialCapacity);
      this.filePaths = new java.util.ArrayList<>(initialCapacity);
      this.positions = new it.unimi.dsi.fastutil.longs.LongArrayList(initialCapacity);
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

    Reader(InputFile input, Function<Record, byte[]> keyEncoder) throws IOException {
      this.stream = input.newStream();
      this.keyEncoder = keyEncoder;

      DataInputStream hd = new DataInputStream(stream);
      byte[] magic = new byte[MAGIC.length];
      hd.readFully(magic);
      if (!Arrays.equals(magic, MAGIC)) {
        throw new IOException("Not a HashIndexHandler file (bad magic)");
      }

      int formatVersion = hd.readInt();
      if (formatVersion != FORMAT_VERSION) {
        throw new IOException("Unsupported HashIndexHandler file version: " + formatVersion);
      }

      this.numBuckets = hd.readInt();
      this.maxBucketSize = hd.readInt();
      int prefixLength = hd.readInt();
      this.maxKeyLength = hd.readInt();
      int maxSuffixLength = hd.readInt();

      byte[] prefix = new byte[prefixLength];
      hd.readFully(prefix);

      this.entrySize = ENTRY_HEADER_BYTES + maxKeyLength + maxSuffixLength;
      this.bucketBytes = (long) maxBucketSize * entrySize;
      this.dataBlockOffset = (long) HEADER_FIXED_LENGTH + prefixLength;
      this.suffixOffset = ENTRY_HEADER_BYTES + maxKeyLength;
      this.prefixStr = new String(prefix, StandardCharsets.UTF_8);
      this.prefixEmpty = prefixStr.isEmpty();
      this.bucketBuf = new byte[Math.toIntExact(bucketBytes)];
      this.paddedKey = new byte[maxKeyLength];
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
      stream.seek(dataBlockOffset + (long) bucket * bucketBytes);
      readFully(stream, bucketBuf, bucketBuf.length);

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

  private static void readFully(InputStream in, byte[] buf, int len) throws IOException {
    int read = 0;
    while (read < len) {
      int n = in.read(buf, read, len - read);
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

