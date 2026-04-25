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

/**
 * Inverted-index file format that uses an "ultra compact hasher" -- a single 32-bit seed shared by
 * all buckets -- to map keys into fixed-width buckets of at most {@code kLimit} entries. Values are
 * stored in fixed-width slots, identical to {@link MinimalPerfectHashFunctionIndexHandler}, so
 * lookup is one seek + one read of {@code count * entrySize} bytes followed by a linear scan of the
 * (very small) bucket.
 *
 * <p>Build algorithm: try seeds {@code 0, 1, 2, ...} until every bucket holds at most {@code
 * kLimit} entries. With {@code numBuckets = ceil(numKeys / kLimit)} the first seed almost always
 * succeeds for any reasonable {@code kLimit} (e.g. 50+), because the expected per-bucket load is
 * {@code kLimit} and the variance is small once {@code kLimit} is in the tens.
 *
 * <p><b>File layout</b> (all integers big-endian):
 *
 * <pre>
 *   [0..5]    MAGIC = "UCHI01"
 *   [6..9]    int32  formatVersion
 *   [10..17]  int64  numKeys
 *   [18..21]  int32  numBuckets
 *   [22..25]  int32  kLimit
 *   [26..29]  int32  seed
 *   [30..33]  int32  prefixLength
 *   [34..37]  int32  maxKeyLength
 *   [38..41]  int32  maxSuffixLength
 *   [42..]    prefix bytes              (prefixLength bytes)
 *   [..]      bucket counts             (numBuckets * uint16, big-endian)
 *   [..EOF]   data block                (numBuckets * kLimit * entrySize bytes)
 * </pre>
 *
 * <p><b>Per-entry layout</b> (matches {@link MinimalPerfectHashFunctionIndexHandler}, total {@code
 * entrySize = 2 + 2 + 8 + maxKeyLength + maxSuffixLength}):
 *
 * <pre>
 *   int16  keyLength
 *   int16  suffixLength
 *   int64  pos
 *   bytes  key    (keyLength bytes)    + padding to maxKeyLength
 *   bytes  suffix (suffixLength bytes) + padding to maxSuffixLength
 * </pre>
 */
public class UltraCompactHasherIndexHandler implements IndexHandler {

  private static final byte[] MAGIC = "UCHI01".getBytes(StandardCharsets.US_ASCII);
  private static final int FORMAT_VERSION = 1;

  private static final int ENTRY_HEADER_BYTES = 2 + 2 + 8;

  /** Fixed-size header up to (but not including) the prefix bytes. */
  private static final int HEADER_FIXED_LENGTH = 6 + 4 + 8 + 4 + 4 + 4 + 4 + 4 + 4;

  /**
   * Per (numBuckets, growth-attempt) iteration: how many seeds we try before deciding the bucket
   * count is too tight. Kept small because, once the load factor is comfortably below 1.0, almost
   * every seed succeeds; if none of these does, growing {@code numBuckets} is far cheaper than
   * burning more seeds.
   */
  private static final int SEED_ATTEMPTS_PER_SIZE = 64;

  /** Maximum times we grow {@code numBuckets} before giving up. */
  private static final int MAX_GROWTH_ATTEMPTS = 32;

  /** Multiplicative growth factor applied to {@code numBuckets} between attempts. */
  private static final double BUCKET_GROWTH_FACTOR = 1.25;

  /**
   * Initial average load (entries / bucket) we aim for, expressed as a fraction of {@code kLimit}.
   * Anything close to 1.0 makes the per-bucket maximum (which is ~ load + Θ(√(load·log
   * numBuckets))) routinely overflow {@code kLimit}; 0.75 leaves enough slack that the very first
   * seed almost always succeeds for any reasonable {@code kLimit}.
   */
  private static final double INITIAL_LOAD_FACTOR = 0.75;

  private static final VarHandle SHORT_BE =
      MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);

  private static final VarHandle LONG_BE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

  private final Function<Record, byte[]> keyEncoder;
  private final long expectedKeyCount;
  private final int kLimit;

  /**
   * @param schema key schema (same supported types as {@link
   *     MinimalPerfectHashFunctionIndexHandler})
   * @param expectedKeyCount sizing hint for writer / reader buffers; must be {@code > 0}
   * @param kLimit maximum number of keys per bucket; must be {@code > 0}
   */
  public UltraCompactHasherIndexHandler(Schema schema, long expectedKeyCount, int kLimit) {
    if (expectedKeyCount <= 0L) {
      throw new IllegalArgumentException("expectedKeyCount must be > 0: " + expectedKeyCount);
    }

    if (kLimit <= 0) {
      throw new IllegalArgumentException("kLimit must be > 0: " + kLimit);
    }

    this.keyEncoder = MinimalPerfectHashFunctionIndexHandler.keyEncoder(schema);
    this.expectedKeyCount = expectedKeyCount;
    this.kLimit = kLimit;
  }

  @Override
  public IndexHandler.Writer writer(OutputFile output) {
    return new Writer(output, keyEncoder, expectedKeyCount, kLimit);
  }

  @Override
  public IndexHandler.Reader reader(InputFile input) throws IOException {
    return new Reader(input, keyEncoder, expectedKeyCount, kLimit);
  }

  // --------------------------------------------------------------------------
  // Hashing
  // --------------------------------------------------------------------------

  /**
   * Mixes the 32-bit hash of {@code key} with {@code seed} via the standard MurmurHash3 finaliser.
   * Returns a non-negative int.
   */
  static int hash(byte[] key, int seed) {
    int h = Arrays.hashCode(key) ^ seed;
    h ^= (h >>> 16);
    h *= 0x85ebca6b;
    h ^= (h >>> 13);
    h *= 0xc2b2ae35;
    h ^= (h >>> 16);
    return h & 0x7FFFFFFF;
  }

  static int bucketOf(byte[] key, int seed, int numBuckets) {
    return hash(key, seed) % numBuckets;
  }

  // --------------------------------------------------------------------------
  // Writer
  // --------------------------------------------------------------------------

  private static class Writer implements IndexHandler.Writer {
    private final OutputFile output;
    private final Function<Record, byte[]> keyEncoder;
    private final int kLimit;
    private final java.util.ArrayList<byte[]> keys;
    private final java.util.ArrayList<String> filePaths;
    private final it.unimi.dsi.fastutil.longs.LongArrayList positions;
    private boolean closed;

    Writer(
        OutputFile output, Function<Record, byte[]> keyEncoder, long expectedKeyCount, int kLimit) {
      this.output = output;
      this.keyEncoder = keyEncoder;
      this.kLimit = kLimit;
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

      keys.add(keyEncoder.apply(key));
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
      // Start a bit below kLimit so the per-bucket max (load + Θ(√(load·log m))) reliably fits;
      // grow numBuckets between attempts if no seed satisfies the cap. See INITIAL_LOAD_FACTOR.
      int targetAvgLoad = Math.max(1, (int) Math.floor(kLimit * INITIAL_LOAD_FACTOR));
      int numBuckets = Math.max(1, (int) Math.ceil((double) n / (double) targetAvgLoad));

      int[] counts = new int[numBuckets];
      int seed = -1;
      int growth = 0;
      while (seed < 0 && growth <= MAX_GROWTH_ATTEMPTS) {
        for (int s = 0; s < SEED_ATTEMPTS_PER_SIZE; s++) {
          Arrays.fill(counts, 0);
          boolean ok = true;
          for (int i = 0; i < n; i++) {
            int b = bucketOf(keys.get(i), s, numBuckets);
            if (++counts[b] > kLimit) {
              ok = false;
              break;
            }
          }

          if (ok) {
            seed = s;
            break;
          }
        }

        if (seed < 0) {
          int grown = Math.max(numBuckets + 1, (int) Math.ceil(numBuckets * BUCKET_GROWTH_FACTOR));
          if (grown <= numBuckets) {
            break;
          }

          numBuckets = grown;
          counts = new int[numBuckets];
          growth++;
        }
      }

      if (seed < 0) {
        throw new IOException(
            "Could not find an UltraCompactHasher (numBuckets, seed) pair after "
                + growth
                + " growth attempts (n="
                + n
                + ", finalNumBuckets="
                + numBuckets
                + ", kLimit="
                + kLimit
                + ")");
      }

      // Encode paths once, compute longest common prefix.
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
      long bucketBytes = (long) kLimit * entrySize;
      long dataLen = (long) numBuckets * bucketBytes;
      byte[] dataBlock = new byte[Math.toIntExact(dataLen)];
      ByteBuffer buf = ByteBuffer.wrap(dataBlock).order(ByteOrder.BIG_ENDIAN);

      // Re-use `counts` as a per-bucket "next free slot" cursor while filling.
      int[] fillCursor = new int[numBuckets];
      int[] finalCounts = new int[numBuckets];
      for (int i = 0; i < n; i++) {
        byte[] key = keys.get(i);
        int b = bucketOf(key, seed, numBuckets);
        int slotInBucket = fillCursor[b]++;
        finalCounts[b] = fillCursor[b];

        long bucketOffset = (long) b * bucketBytes;
        int off = Math.toIntExact(bucketOffset + (long) slotInBucket * entrySize);
        byte[] path = pathBytes[i];
        int suffixLength = path.length - prefix.length;
        buf.putShort(off, (short) key.length);
        buf.putShort(off + 2, (short) suffixLength);
        buf.putLong(off + 4, positions.getLong(i));
        System.arraycopy(key, 0, dataBlock, off + ENTRY_HEADER_BYTES, key.length);
        System.arraycopy(
            path, prefix.length, dataBlock, off + ENTRY_HEADER_BYTES + maxKeyLength, suffixLength);

        keys.set(i, null);
        pathBytes[i] = null;
      }

      keys.clear();
      positions.clear();

      // Bucket counts table: uint16 per bucket (kLimit must fit in uint16 for this format).
      if (kLimit > 0xFFFF) {
        throw new IOException("kLimit " + kLimit + " exceeds uint16 limit (65535)");
      }

      byte[] countsBlock = new byte[numBuckets * 2];
      ByteBuffer cbuf = ByteBuffer.wrap(countsBlock).order(ByteOrder.BIG_ENDIAN);
      for (int b = 0; b < numBuckets; b++) {
        cbuf.putShort(b * 2, (short) finalCounts[b]);
      }

      try (OutputStream os = output.create();
          DataOutputStream dos = new DataOutputStream(os)) {
        dos.write(MAGIC);
        dos.writeInt(FORMAT_VERSION);
        dos.writeLong(n);
        dos.writeInt(numBuckets);
        dos.writeInt(kLimit);
        dos.writeInt(seed);
        dos.writeInt(prefix.length);
        dos.writeInt(maxKeyLength);
        dos.writeInt(maxSuffixLength);
        dos.write(prefix);
        dos.write(countsBlock);
        dos.write(dataBlock);
        dos.flush();
      }
    }
  }

  // --------------------------------------------------------------------------
  // Reader
  // --------------------------------------------------------------------------

  private static class Reader implements IndexHandler.Reader {
    private final SeekableInputStream stream;
    private final Function<Record, byte[]> keyEncoder;
    private final int numBuckets;
    private final int seed;
    private final int entrySize;
    private final long bucketBytes;
    private final long dataBlockOffset;
    private final int suffixOffset;
    private final String prefixStr;

    /** uint16 per bucket. Kept in memory for O(1) count lookup. */
    private final byte[] countsBlock;

    /** Per-Reader scratch buffer reused by every {@link #lookup(Record)} call. */
    private final byte[] bucketBuf;

    Reader(
        InputFile input,
        Function<Record, byte[]> keyEncoder,
        long expectedKeyCount,
        int expectedKLimit)
        throws IOException {
      this.stream = input.newStream();
      this.keyEncoder = keyEncoder;

      // 1) Speculative bulk read covering the fixed header + (most of) the metadata region
      //    (prefix bytes + bucket-counts table). The exact metadata size depends on the
      //    `prefixLength` and `numBuckets` recorded in the header, but we can estimate
      //    `numBuckets ~= ceil(expectedKeyCount / expectedKLimit)` and reserve a small slack
      //    for the path prefix. Issuing one large read up front, rather than 6+ tiny reads
      //    via DataInputStream, collapses Reader-open into a single underlying read() on
      //    object stores. Under-estimates fall back to one extra tail read; over-estimates
      //    waste a few KB of transient buffer.
      int prefetch = getPrefetch(input, (double) expectedKeyCount, expectedKLimit);

      byte[] prefetched = readFully(stream, prefetch);

      // Parse fixed header out of the prefetched buffer.
      ByteBuffer hd = ByteBuffer.wrap(prefetched).order(ByteOrder.BIG_ENDIAN);
      byte[] magic = new byte[MAGIC.length];
      hd.get(magic);
      if (!Arrays.equals(magic, MAGIC)) {
        throw new IOException("Not an UltraCompactHasher index file (bad magic)");
      }

      int formatVersion = hd.getInt();
      if (formatVersion != FORMAT_VERSION) {
        throw new IOException(
            "Unsupported UltraCompactHasher index file version: " + formatVersion);
      }

      hd.getLong(); // numKeys (unused at read time)
      this.numBuckets = hd.getInt();
      int kLimit = hd.getInt();
      this.seed = hd.getInt();
      int prefixLength = hd.getInt();
      int maxKeyLength = hd.getInt();
      int maxSuffixLength = hd.getInt();

      // 2) Materialise the metadata region (prefix bytes + bucket-counts table). In the common
      //    case it already sits inside `prefetched`; otherwise read just the missing tail.
      int metadataRegionLength = Math.addExact(prefixLength, numBuckets * 2);
      int prefetchedMetadata = prefetched.length - HEADER_FIXED_LENGTH;
      byte[] metadataRegion;
      if (prefetchedMetadata >= metadataRegionLength) {
        metadataRegion =
            Arrays.copyOfRange(
                prefetched, HEADER_FIXED_LENGTH, HEADER_FIXED_LENGTH + metadataRegionLength);
      } else {
        metadataRegion = new byte[metadataRegionLength];
        System.arraycopy(prefetched, HEADER_FIXED_LENGTH, metadataRegion, 0, prefetchedMetadata);
        readFully(
            stream, metadataRegion, prefetchedMetadata, metadataRegionLength - prefetchedMetadata);
      }

      byte[] prefix = Arrays.copyOfRange(metadataRegion, 0, prefixLength);
      this.countsBlock = Arrays.copyOfRange(metadataRegion, prefixLength, metadataRegionLength);

      this.entrySize = ENTRY_HEADER_BYTES + maxKeyLength + maxSuffixLength;
      this.bucketBytes = (long) kLimit * entrySize;
      this.dataBlockOffset = (long) HEADER_FIXED_LENGTH + prefixLength + (long) numBuckets * 2L;
      this.suffixOffset = ENTRY_HEADER_BYTES + maxKeyLength;
      this.prefixStr = new String(prefix, StandardCharsets.UTF_8);
      this.bucketBuf = new byte[Math.toIntExact(bucketBytes)];
    }

    private int getPrefetch(InputFile input, double expectedKeyCount, double expectedKLimit)
        throws IOException {
      long fileLength;
      try {
        fileLength = input.getLength();
      } catch (RuntimeException e) {
        fileLength = Long.MAX_VALUE;
      }

      long estimatedNumBuckets = Math.max(1L, (long) Math.ceil(expectedKeyCount / expectedKLimit));
      // 4 KB slack covers the longest-common path prefix in virtually all real workloads.
      long estimatedMetadata = estimatedNumBuckets * 2L + 4096L;
      int prefetch =
          (int)
              Math.min(
                  (long) HEADER_FIXED_LENGTH + estimatedMetadata,
                  Math.min((long) Integer.MAX_VALUE - 16L, fileLength));
      if (prefetch < HEADER_FIXED_LENGTH) {
        throw new IOException(
            "File too short to contain UltraCompactHasher index header: " + fileLength);
      }
      return prefetch;
    }

    @Override
    public IndexHandler.Hit lookup(Record key) throws IOException {
      byte[] encoded = keyEncoder.apply(key);
      int bucket = bucketOf(encoded, seed, numBuckets);
      int count = ((short) SHORT_BE.get(countsBlock, bucket * 2)) & 0xFFFF;
      if (count == 0) {
        return null;
      }

      int readLen = count * entrySize;
      stream.seek(dataBlockOffset + (long) bucket * bucketBytes);
      readFully(stream, bucketBuf, readLen);

      for (int i = 0; i < count; i++) {
        int off = i * entrySize;
        int storedKeyLength = ((short) SHORT_BE.get(bucketBuf, off)) & 0xFFFF;
        if (storedKeyLength != encoded.length) {
          continue;
        }

        int mismatch =
            Arrays.mismatch(
                bucketBuf,
                off + ENTRY_HEADER_BYTES,
                off + ENTRY_HEADER_BYTES + storedKeyLength,
                encoded,
                0,
                encoded.length);
        if (mismatch >= 0) {
          continue;
        }

        int suffixLength = ((short) SHORT_BE.get(bucketBuf, off + 2)) & 0xFFFF;
        long pos = (long) LONG_BE.get(bucketBuf, off + 4);

        String filePath;
        if (prefixStr.isEmpty()) {
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

  private static byte[] readFully(InputStream in, int len) throws IOException {
    byte[] buf = new byte[len];
    readFully(in, buf, 0, len);
    return buf;
  }

  private static void readFully(InputStream in, byte[] buf, int len) throws IOException {
    readFully(in, buf, 0, len);
  }

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
