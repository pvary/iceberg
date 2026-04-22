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

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

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
public class MinimalPerfectHashFunctionIndexHandler implements IndexHandler {

  private static final byte[] MAGIC = "MPHFI1".getBytes(StandardCharsets.US_ASCII);
  private static final int FORMAT_VERSION = 1;

  /**
   * Per-entry overhead (keyLength + suffixLength + pos), excluding the padded key + suffix bytes.
   */
  private static final int ENTRY_HEADER_BYTES = 2 + 2 + 8;

  /** Fixed-size file header: everything up to (but not including) the prefix bytes. */
  private static final int HEADER_FIXED_LENGTH = 6 + 4 + 8 + 4 + 4 + 4 + 8;

  /**
   * Big-endian byte-array view handles used by the reader hot path to decode the per-entry header
   * fields directly out of a {@code byte[]} without allocating a {@link ByteBuffer} per lookup.
   * Both lower to JVM intrinsics on x86 / aarch64.
   */
  private static final VarHandle SHORT_BE =
      MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);

  private static final VarHandle LONG_BE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

  private final Function<Record, byte[]> keyEncoder;

  /**
   * Creates a handler bound to a key {@link Schema}. The schema must contain at least one field and
   * every field must be one of the supported primitive types ({@code long}, {@code int}, {@code
   * string}, {@code uuid}); the fields can appear in any order and any combination.
   *
   * <p>The schema is compiled once into a per-record encoder that produces the canonical {@code
   * byte[]} representation used by the MPHF (see {@link #keyEncoder(Schema)}).
   */
  public MinimalPerfectHashFunctionIndexHandler(Schema schema) {
    this.keyEncoder = keyEncoder(schema);
  }

  @Override
  public IndexHandler.Writer writer(OutputFile output) {
    return new Writer(output, keyEncoder);
  }

  @Override
  public IndexHandler.Reader reader(InputFile input) throws IOException {
    return new Reader(input, keyEncoder);
  }

  /**
   * Compiles {@code schema} into a {@link Function} that encodes a {@link Record} matching that
   * schema into the canonical key {@code byte[]}: each field is appended in declaration order using
   * a fixed per-type layout (see {@link #fieldEncoder(Type, int)}). Unsupported types are rejected
   * at construction time.
   */
  static Function<Record, byte[]> keyEncoder(Schema schema) {
    if (schema == null || schema.columns().isEmpty()) {
      throw new IllegalArgumentException("Key schema must contain at least one field");
    }

    List<Types.NestedField> fields = schema.columns();
    int n = fields.size();
    FieldEncoder[] encoders = new FieldEncoder[n];
    int fixedSize = 0;
    boolean allFixed = true;
    for (int i = 0; i < n; i++) {
      encoders[i] = fieldEncoder(fields.get(i).type(), i);
      int width = encoders[i].width();
      if (width < 0) {
        allFixed = false;
      } else {
        fixedSize += width;
      }
    }

    final int initialCapacity = allFixed ? fixedSize : Math.max(16, fixedSize + 16);
    return record -> {
      if (record == null) {
        throw new IllegalArgumentException("Key record cannot be null");
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream(initialCapacity);
      try (DataOutputStream dos = new DataOutputStream(baos)) {
        for (int i = 0; i < encoders.length; i++) {
          encoders[i].write(record.get(i), dos);
        }
      } catch (IOException e) {
        // ByteArrayOutputStream cannot throw, but DataOutputStream signature requires it.
        throw new IllegalStateException("Failed to encode key", e);
      }

      return baos.toByteArray();
    };
  }

  /**
   * Returns the encoder for a single field. Currently supports {@code long} (8 BE bytes), {@code
   * int} (4 BE bytes), {@code string} (UTF-8 bytes, no length prefix), and {@code uuid} (16 bytes,
   * msb long first). The {@code position} parameter is only used for error messages.
   */
  private static FieldEncoder fieldEncoder(Type type, int position) {
    return switch (type.typeId()) {
      case LONG ->
          new FieldEncoder() {
            @Override
            public void write(Object value, DataOutputStream out) throws IOException {
              out.writeLong(requireType(value, Long.class, position));
            }

            @Override
            public int width() {
              return 8;
            }
          };
      case INTEGER ->
          new FieldEncoder() {
            @Override
            public void write(Object value, DataOutputStream out) throws IOException {
              out.writeInt(requireType(value, Integer.class, position));
            }

            @Override
            public int width() {
              return 4;
            }
          };
      case STRING ->
          new FieldEncoder() {
            @Override
            public void write(Object value, DataOutputStream out) throws IOException {
              CharSequence cs = requireType(value, CharSequence.class, position);
              byte[] bytes = cs.toString().getBytes(StandardCharsets.UTF_8);
              if (bytes.length > 0xFFFF) {
                throw new IOException(
                    "String key field "
                        + position
                        + " is "
                        + bytes.length
                        + " UTF-8 bytes, exceeds uint16 limit (65535)");
              }

              // 2-byte big-endian length prefix so consecutive variable-length fields are
              // unambiguous (e.g. <string, string> with ("ab", "c") vs ("a", "bc")).
              out.writeShort(bytes.length);
              out.write(bytes);
            }

            @Override
            public int width() {
              return -1;
            }
          };
      case UUID ->
          new FieldEncoder() {
            @Override
            public void write(Object value, DataOutputStream out) throws IOException {
              UUID uuid = requireType(value, UUID.class, position);
              out.writeLong(uuid.getMostSignificantBits());
              out.writeLong(uuid.getLeastSignificantBits());
            }

            @Override
            public int width() {
              return 16;
            }
          };
      default ->
          throw new IllegalArgumentException(
              "Unsupported key field type at position "
                  + position
                  + ": "
                  + type
                  + " (expected long, int, string, or uuid)");
    };
  }

  private static <T> T requireType(Object value, Class<T> expected, int position) {
    if (value == null) {
      throw new IllegalArgumentException("Key field " + position + " is null");
    } else if (!expected.isInstance(value)) {
      throw new IllegalArgumentException(
          "Key field "
              + position
              + " expected "
              + expected.getSimpleName()
              + ", got "
              + value.getClass().getName());
    }

    return expected.cast(value);
  }

  /** Per-field encoder. {@code fixedWidth} returns -1 for variable-length types (e.g. string). */
  private interface FieldEncoder {
    void write(Object value, DataOutputStream out) throws IOException;

    int width();
  }

  private static class Writer implements IndexHandler.Writer {
    private final OutputFile output;
    private final Function<Record, byte[]> keyEncoder;
    private final List<byte[]> keys = Lists.newArrayList();
    private final List<String> filePaths = Lists.newArrayList();
    private final LongArrayList positions = new LongArrayList();
    private boolean closed;

    Writer(OutputFile output, Function<Record, byte[]> keyEncoder) {
      this.output = output;
      this.keyEncoder = keyEncoder;
    }

    /**
     * Adds one entry to the index. Buffered in memory; the GOVMinimalPerfectHashFunction is built
     * and the file is written when {@link #close()} is called. Each {@code key} must be unique
     * within a single Writer instance.
     *
     * <p>The supplied {@link Record} must match the key schema passed to the handler constructor;
     * it is encoded to {@code byte[]} via the schema-derived encoder built by {@link
     * #keyEncoder(Schema)}.
     */
    @Override
    public void add(Record key, String filePath, long pos) {
      if (closed) {
        throw new IllegalStateException("Writer already closed");
      }

      keys.add(keyEncoder.apply(key));
      filePaths.add(filePath);
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

      // Encode every path to UTF-8 bytes once, then compute the longest common byte prefix over
      // the encoded form. Encoded bytes replace the String references row-by-row to keep peak
      // memory low.
      int n = filePaths.size();
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
      // row's key + suffix bytes are copied into the block we null the slot in `keys` /
      // `pathBytes` so the underlying byte[]s become eligible for GC; for large indexes this
      // halves peak memory because the input buffers and the output dataBlock no longer have to
      // be live at the same time.
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
        byte[] path = pathBytes[i];
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
        pathBytes[i] = null;
      }

      // Input buffers are no longer needed - drop the backing arrays entirely.
      keys.clear();
      positions.clear();

      // Serialise the hash function to memory so we know its length up front.
      byte[] hashFunctionBlob;
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream(1 << 20);
          ObjectOutputStream oos = new ObjectOutputStream(baos)) {
        oos.writeObject(hashFunction);
        oos.flush();
        hashFunctionBlob = baos.toByteArray();
      }

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

  private static class Reader implements IndexHandler.Reader {
    private final SeekableInputStream stream;
    private final Header header;
    private final Function<Record, byte[]> keyEncoder;
    private final GOVMinimalPerfectHashFunction<byte[]> hashFunction;
    private final long dataBlockOffset;
    private final int entrySize;

    /**
     * Per-Reader scratch buffer reused by every {@link #lookup(Record)} call so the hot path does
     * not allocate a fresh {@code byte[entrySize]} per invocation. Readers are not thread-safe;
     * concurrent callers must hold their own Reader instances (which is already the case for the
     * one-Reader-per-lookup pattern used by the benchmark and the only sensible production use).
     */
    private final byte[] entryBuf;

    /**
     * UTF-8 string form of {@link Header#prefix}, decoded once at construction. Lets the lookup
     * path avoid re-decoding the prefix bytes on every call and skip an intermediate concat-buffer
     * allocation when assembling the full file path.
     */
    private final String prefixStr;

    /** Cached {@code ENTRY_HEADER_BYTES + maxKeyLength}: start offset of the suffix bytes. */
    private final int suffixOffset;

    @SuppressWarnings({"unchecked", "DangerousJavaDeserialization"})
    Reader(InputFile input, Function<Record, byte[]> keyEncoder) throws IOException {
      this.stream = input.newStream();
      this.keyEncoder = keyEncoder;

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
      this.entryBuf = new byte[entrySize];
      this.prefixStr = new String(prefix, StandardCharsets.UTF_8);
      this.suffixOffset = ENTRY_HEADER_BYTES + maxKeyLength;
    }

    /**
     * Looks up a key. Returns {@code null} for keys that did not participate in the build (the
     * stored key is byte-compared against {@code key} so false positives are eliminated).
     *
     * <p>One MPHF call followed by a single seek + read of {@code entrySize} bytes from the
     * underlying file (no data-block caching). The hot path allocates only the result objects (the
     * path {@link String} and the {@link HitImpl}) plus whatever {@link #keyEncoder} allocates for
     * the encoded key bytes.
     */
    @Override
    public IndexHandler.Hit lookup(Record key) throws IOException {
      byte[] encoded = keyEncoder.apply(key);
      long slot = hashFunction.getLong(encoded);
      if (slot < 0L || slot >= header.numKeys) {
        return null;
      }

      // Read the entry into the per-Reader scratch buffer (no per-call allocation).
      stream.seek(dataBlockOffset + slot * entrySize);
      readFully(stream, entryBuf, entrySize);

      // Decode the per-entry header fields directly out of the byte[] via VarHandles, avoiding
      // the ByteBuffer.wrap(...) allocation the previous version paid per lookup.
      int storedKeyLength = ((short) SHORT_BE.get(entryBuf, 0)) & 0xFFFF;
      int suffixLength = ((short) SHORT_BE.get(entryBuf, 2)) & 0xFFFF;
      long pos = (long) LONG_BE.get(entryBuf, 4);

      // Verify the stored key matches the looked-up key. Arrays.mismatch is a JIT intrinsic that
      // lowers to a vectorised compare on x86/aarch64; much faster than a per-byte loop for
      // medium keys (UUID/composite).
      if (storedKeyLength != encoded.length) {
        return null;
      }

      int mismatch =
          Arrays.mismatch(
              entryBuf,
              ENTRY_HEADER_BYTES,
              ENTRY_HEADER_BYTES + storedKeyLength,
              encoded,
              0,
              encoded.length);
      if (mismatch >= 0) {
        return null;
      }

      // Materialise the path. When there is no common prefix decoding the suffix directly
      // skips an intermediate byte[] allocation + arraycopy.
      String filePath;
      if (prefixStr.isEmpty()) {
        filePath = new String(entryBuf, suffixOffset, suffixLength, StandardCharsets.UTF_8);
      } else {
        // Use cached prefixStr so we do not re-decode the prefix bytes on every lookup.
        filePath =
            prefixStr + new String(entryBuf, suffixOffset, suffixLength, StandardCharsets.UTF_8);
      }

      return new HitImpl(filePath, pos);
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }

  private static byte[] readFully(InputStream in, int len) throws IOException {
    byte[] buf = new byte[len];
    readFully(in, buf, len);
    return buf;
  }

  /**
   * Fills the first {@code len} bytes of {@code buf} from {@code in}. Used by the reader hot path
   * with a per-Reader scratch buffer to keep {@link Reader#lookup(Record)} allocation-free.
   */
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

  /**
   * Computes the longest common byte prefix of {@code paths}. Returns an empty array for an empty
   * input or when no byte is shared by all entries.
   */
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

  private record Header(
      int formatVersion,
      long numKeys,
      long hashFunctionLength,
      int maxKeyLength,
      int maxSuffixLength,
      byte[] prefix) {}
}
