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
package org.apache.iceberg.mphf;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Iceberg-specific MPHF-based inverted-index file format, backed by Sux4J's {@link
 * GOVMinimalPerfectHashFunction}.
 *
 * <p>The hash function maps each known key to a unique slot in {@code [0, numKeys)}; the per-key
 * payload (full key, file-path suffix, position) is stored at that slot in a fixed-width data
 * block. Both the key and the path suffix are padded to the longest occurrence so every entry has
 * the same width and the data block can be indexed directly by {@code slot * entrySize} — no
 * offsets table needed.
 *
 * <p><b>Per-entry layout</b> (big-endian, total {@code entrySize = 2 + 2 + 8 + maxKeyLength +
 * maxSuffixLength}):
 *
 * <pre>
 *   int16  keyLength        (actual byte length of this entry's key)
 *   int16  suffixLength     (actual byte length of this entry's path suffix)
 *   int64  pos
 *   bytes  key    (keyLength bytes)    + (maxKeyLength   - keyLength)   zero pad bytes
 *   bytes  suffix (suffixLength bytes) + (maxSuffixLength - suffixLength) zero pad bytes
 * </pre>
 *
 * <p>For keys not in the build set the MPHF returns an arbitrary slot in range; the stored key is
 * compared against the looked-up key to reject false positives unambiguously.
 *
 * <p><b>File layout</b> (all integers big-endian, single sequential read):
 *
 * <pre>
 *   [0..5]    MAGIC = "MPHFI1"
 *   [6..9]    int32  formatVersion
 *   [10..17]  int64  numKeys
 *   [18..21]  int32  prefixLength         (UTF-8 bytes of longest common path prefix)
 *   [22..25]  int32  maxKeyLength            (longest key, in bytes)
 *   [26..29]  int32  maxSuffixLength         (longest path suffix, in bytes)
 *   [30..37]  int64  hashFunctionLength   (bytes of the GOVMinimalPerfectHashFunction blob)
 *   [38..]    prefix bytes                (prefixLength bytes)
 *   [..]      hash function blob          (hashFunctionLength bytes, Java object stream)
 *   [..EOF]   data block                  (numKeys * entrySize bytes, see per-entry layout above)
 * </pre>
 */
public class MinimalPerfectHashFunctionIndexFile {

  private static final byte[] MAGIC = "MPHFI1".getBytes(StandardCharsets.US_ASCII);
  private static final int FORMAT_VERSION = 1;

  /**
   * Per-entry overhead (keyLength + suffixLength + pos), excluding the padded key + suffix bytes.
   */
  private static final int ENTRY_HEADER_BYTES = 2 + 2 + 8;

  /** Fixed-size file header: everything up to (but not including) the prefix bytes. */
  private static final int HEADER_FIXED_LENGTH = 6 + 4 + 8 + 4 + 4 + 4 + 8;

  private MinimalPerfectHashFunctionIndexFile() {}

  public static class Writer implements Closeable {
    private final OutputFile output;
    private final List<byte[]> keys = Lists.newArrayList();
    private final List<byte[]> filePathBytes = Lists.newArrayList();
    private final LongArrayList positions = new LongArrayList();
    private boolean closed;

    public Writer(OutputFile output) {
      this.output = output;
    }

    /**
     * Adds one entry to the index. Buffered in memory; the GOVMinimalPerfectHashFunction is built
     * and the file is written when {@link #close()} is called. Each {@code key} must be unique
     * within a single Writer instance.
     */
    public void add(byte[] key, String filePath, long pos) {
      if (closed) {
        throw new IllegalStateException("Writer already closed");
      }

      keys.add(key);
      filePathBytes.add(filePath.getBytes(StandardCharsets.UTF_8));
      positions.add(pos);
    }

    /** Builds the MPHF over the buffered keys and writes the entire file. */
    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }

      closed = true;

      GOVMinimalPerfectHashFunction<byte[]> hashFunction;
      try {
        hashFunction =
            new GOVMinimalPerfectHashFunction.Builder<byte[]>()
                .keys(keys)
                .transform(TransformationStrategies.byteArray())
                .build();
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException("Failed to build GOVMinimalPerfectHashFunction", e);
      }

      // Compute the longest common prefix and the longest suffix length to pick the per-entry
      // width.
      byte[] prefix = longestCommonPrefix(filePathBytes);
      int maxSuffixLength = 0;
      for (byte[] p : filePathBytes) {
        int suffixLength = p.length - prefix.length;
        if (suffixLength > maxSuffixLength) {
          maxSuffixLength = suffixLength;
        }
      }

      if (maxSuffixLength > 0xFFFF) {
        throw new IOException(
            "Path suffix " + maxSuffixLength + " bytes exceeds int16 limit (65535)");
      }

      // Longest key dictates the per-entry key slot width.
      int maxKeyLength = 0;
      for (byte[] key : keys) {
        if (key.length > maxKeyLength) {
          maxKeyLength = key.length;
        }
      }

      if (maxKeyLength > 0xFFFF) {
        throw new IOException("Key " + maxKeyLength + " bytes exceeds int16 limit (65535)");
      }

      int entrySize = ENTRY_HEADER_BYTES + maxKeyLength + maxSuffixLength;

      // Build the data block: each key goes into the slot returned by the MPHF. As soon as a
      // row's key + path bytes are copied into the block we null the slot in `keys` /
      // `filePathBytes` so the underlying byte[]s become eligible for GC; for large indexes this
      // halves peak memory because the input buffers and the output dataBlock no longer have to
      // be live at the same time.
      int n = keys.size();
      byte[] dataBlock = new byte[Math.toIntExact((long) n * entrySize)];
      ByteBuffer buf = ByteBuffer.wrap(dataBlock).order(ByteOrder.BIG_ENDIAN);
      for (int i = 0; i < n; i++) {
        byte[] key = keys.get(i);
        long slot = hashFunction.getLong(key);
        if (slot < 0L || slot >= n) {
          throw new IOException(
              "Minimal Perfect Hash Function returned out-of-range slot " + slot + " for key " + i);
        }

        int off = Math.toIntExact(slot * entrySize);
        byte[] path = filePathBytes.get(i);
        int suffixLength = path.length - prefix.length;
        buf.putShort(off, (short) key.length);
        buf.putShort(off + 2, (short) suffixLength);
        buf.putLong(off + 4, positions.getLong(i));
        // key bytes (padded by leaving the rest as 0)
        System.arraycopy(key, 0, dataBlock, off + ENTRY_HEADER_BYTES, key.length);
        // suffix bytes (padded by leaving the rest as 0)
        System.arraycopy(
            path, prefix.length, dataBlock, off + ENTRY_HEADER_BYTES + maxKeyLength, suffixLength);

        // Drop the references now that the bytes are in dataBlock.
        keys.set(i, null);
        filePathBytes.set(i, null);
      }

      // Input buffers are no longer needed - drop the backing arrays entirely.
      keys.clear();
      filePathBytes.clear();
      positions.clear();

      // Serialise the hash function to memory so we know its length up front.
      byte[] hashFunctionBlob;
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream(1 << 20);
          ObjectOutputStream oos = new ObjectOutputStream(baos)) {
        oos.writeObject(hashFunction);
        oos.flush();
        hashFunctionBlob = baos.toByteArray();
      }

      // Hash function is fully serialised; release it so only dataBlock + hashFunctionBlob
      // remain live during the actual file write.
      hashFunction = null;

      try (OutputStream os = output.create();
          DataOutputStream dos = new DataOutputStream(os)) {
        dos.write(MAGIC);
        dos.writeInt(FORMAT_VERSION);
        dos.writeLong(n);
        dos.writeInt(prefix.length);
        dos.writeInt(maxKeyLength);
        dos.writeInt(maxSuffixLength);
        dos.writeLong(hashFunctionBlob.length);
        dos.write(prefix);
        dos.write(hashFunctionBlob);
        dos.write(dataBlock);
        dos.flush();
      }
    }
  }

  // -----------------------------------------------------------------------
  // Reader
  // -----------------------------------------------------------------------

  public static class Reader implements AutoCloseable {
    private final SeekableInputStream stream;
    private final Header header;
    private final GOVMinimalPerfectHashFunction<byte[]> hashFunction;
    private final long dataBlockOffset;
    private final int entrySize;

    /** Result of a successful {@link #lookup(byte[])}: {@code (filePath, pos)}. */
    public static class Hit {
      public final String filePath;
      public final long pos;

      public Hit(String filePath, long pos) {
        this.filePath = filePath;
        this.pos = pos;
      }
    }

    @SuppressWarnings({"unchecked", "DangerousJavaDeserialization"})
    public Reader(InputFile input) throws IOException {
      this.stream = input.newStream();

      // 1) Fixed header: MAGIC + version + numKeys + all length fields.
      byte[] fixed = readFully(stream, HEADER_FIXED_LENGTH);
      DataInputStream hd = new DataInputStream(new ByteArrayInputStream(fixed));
      byte[] magic = new byte[MAGIC.length];
      hd.readFully(magic);
      if (!checkMagic(magic)) {
        throw new IOException("Not an MinimalPerfectHashFunctionIndexFile (bad magic)");
      }

      int formatVersion = hd.readInt();
      if (formatVersion != FORMAT_VERSION) {
        throw new IOException(
            "Unsupported MinimalPerfectHashFunctionIndexFile version: " + formatVersion);
      }

      long numKeys = hd.readLong();
      int prefixLength = hd.readInt();
      int maxKeyLength = hd.readInt();
      int maxSuffixLength = hd.readInt();
      long hashFunctionLength = hd.readLong();

      // 2) Variable-length prefix bytes.
      byte[] prefix = readFully(stream, prefixLength);

      this.header =
          new Header(
              formatVersion, numKeys, hashFunctionLength, maxKeyLength, maxSuffixLength, prefix);

      // 3) Hash function blob.
      byte[] hashFunctionBlob = readFully(stream, Math.toIntExact(hashFunctionLength));
      try (ObjectInputStream ois =
          new ObjectInputStream(new ByteArrayInputStream(hashFunctionBlob))) {
        this.hashFunction = (GOVMinimalPerfectHashFunction<byte[]>) ois.readObject();
      } catch (ClassNotFoundException e) {
        throw new IOException("Failed to deserialize hash function", e);
      }

      // 4) Data block stays on disk; remember where it starts and the per-entry size so each
      //    lookup can seek + read just one entry.
      this.entrySize = ENTRY_HEADER_BYTES + maxKeyLength + maxSuffixLength;
      this.dataBlockOffset = (long) HEADER_FIXED_LENGTH + prefixLength + hashFunctionLength;
    }

    /**
     * Looks up a key. Returns {@code null} for keys that did not participate in the build (the
     * stored key is byte-compared against {@code key} so false positives are eliminated).
     *
     * <p>One MPHF call followed by a single seek + read of {@code entrySize} bytes from the
     * underlying file (no data-block caching).
     */
    public Hit lookup(byte[] key) throws IOException {
      long slot = hashFunction.getLong(key);
      if (slot < 0L || slot >= header.numKeys) {
        return null;
      }

      long entryOffset = dataBlockOffset + slot * entrySize;
      stream.seek(entryOffset);
      byte[] entry = readFully(stream, entrySize);

      ByteBuffer buf = ByteBuffer.wrap(entry).order(ByteOrder.BIG_ENDIAN);
      int storedKeyLength = buf.getShort(0) & 0xFFFF;
      int suffixLength = buf.getShort(2) & 0xFFFF;
      long pos = buf.getLong(4);

      // Verify the stored key matches the looked-up key.
      if (storedKeyLength != key.length) {
        return null;
      }

      for (int i = 0; i < storedKeyLength; i++) {
        if (entry[ENTRY_HEADER_BYTES + i] != key[i]) {
          return null;
        }
      }

      byte[] full = new byte[header.prefix.length + suffixLength];
      System.arraycopy(header.prefix, 0, full, 0, header.prefix.length);
      System.arraycopy(
          entry,
          ENTRY_HEADER_BYTES + header.maxKeyLength,
          full,
          header.prefix.length,
          suffixLength);
      return new Hit(new String(full, StandardCharsets.UTF_8), pos);
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }

  /** Computes the longest common UTF-8 byte prefix of {@code paths}. */
  private static byte[] longestCommonPrefix(List<byte[]> paths) {
    if (paths.isEmpty()) {
      return new byte[0];
    }

    byte[] first = paths.get(0);
    int prefixLength = first.length;
    for (int i = 1; i < paths.size() && prefixLength > 0; i++) {
      byte[] cur = paths.get(i);
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

  private static byte[] readFully(InputStream in, int len) throws IOException {
    byte[] buf = new byte[len];
    int read = 0;
    while (read < len) {
      int n = in.read(buf, read, len - read);
      if (n < 0) {
        throw new EOFException("Unexpected EOF after " + read + " of " + len + " bytes");
      }

      read += n;
    }

    return buf;
  }

  private static boolean checkMagic(byte[] candidate) {
    if (candidate.length != MAGIC.length) {
      return false;
    }

    for (int i = 0; i < MAGIC.length; i++) {
      if (candidate[i] != MAGIC[i]) {
        return false;
      }
    }

    return true;
  }

  private record Header(
      int formatVersion,
      long numKeys,
      long hashFunctionLength,
      int maxKeyLength,
      int maxSuffixLength,
      byte[] prefix) {}
}
