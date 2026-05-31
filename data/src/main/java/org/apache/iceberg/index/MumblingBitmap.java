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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Locale;

/**
 * A two-level bitmap for 16-bit positions.
 *
 * <p>The high byte of a position selects one of 256 mini-containers, and the low byte is stored
 * within that container. Each mini-container starts as a {@code Sparse} sorted list of up to 31
 * values and is promoted to a {@code Dense} 256-bit bitmap once it would grow beyond that capacity.
 *
 * <p>This is a Java port of the {@code mumbling.rs} reference implementation. Rust unsigned types
 * ({@code u8}, {@code u16}, {@code u64}) are represented with Java signed primitives and unsigned
 * arithmetic where required.
 */
public class MumblingBitmap {
  private static final int MAX_POSITION = 400_000;
  private static final int NUM_KEYS = (MAX_POSITION + 255) / 256;
  private static final int SPARSE_CAPACITY = 31;
  private static final int MAX_SIZE = 255;

  /** Number of values stored in each mini-container (unsigned byte, 0..255). */
  private final short[] sizes;

  private final MiniContainer[] containers;

  public MumblingBitmap() {
    this.sizes = new short[NUM_KEYS];
    this.containers = new MiniContainer[NUM_KEYS];
    for (int i = 0; i < NUM_KEYS; i++) {
      this.containers[i] = new SparseContainer();
    }
  }

  /** Returns whether the given position is set in the bitmap. */
  public boolean isSet(int pos) {
    checkPosition(pos);

    int key = pos >>> 8;
    int size = sizes[key];
    if (size > 0) {
      return containers[key].contains(pos & 0xFF, size);
    } else {
      return false;
    }
  }

  /**
   * Sets the given position in the bitmap.
   *
   * @return {@code true} if the position was newly added, {@code false} if it was already set
   */
  public boolean set(int pos) {
    checkPosition(pos);

    int key = pos >>> 8;
    int val = pos & 0xFF;
    int size = sizes[key];

    boolean added = false;
    if (size < SPARSE_CAPACITY) {
      // the mini-container will still be sparse
      MiniContainer container = containers[key];
      if (container instanceof SparseContainer) {
        added = ((SparseContainer) container).insert(val, this, key);
      } else {
        throw new IllegalStateException(
            String.format(Locale.ROOT, "Found dense mini-container for %d values", size));
      }
    } else {
      if (size == SPARSE_CAPACITY && !containers[key].contains(val, size)) {
        // convert the mini-container to dense
        containers[key] = ((SparseContainer) containers[key]).toDense();
      }

      if (containers[key] instanceof DenseContainer) {
        added = ((DenseContainer) containers[key]).insert(val);
        if (added && sizes[key] < MAX_SIZE) {
          sizes[key] = (short) (sizes[key] + 1);
        }
      }
    }

    return added;
  }

  /** Returns the number of positions set in the bitmap. */
  public int cardinality() {
    int total = 0;
    for (int i = 0; i < NUM_KEYS; i++) {
      total += containers[i].cardinality(sizes[i]);
    }

    return total;
  }

  /** Returns the serialized size of the bitmap in bytes. */
  public int size() {
    int total = NUM_KEYS; // size bytes
    for (short size : sizes) {
      total += size > SPARSE_CAPACITY ? 32 : size;
    }

    return total;
  }

  /** Serializes the bitmap into a newly allocated {@link ByteBuffer} ready for reading. */
  public ByteBuffer serialize() {
    ByteBuffer buffer = ByteBuffer.allocate(size());
    for (int i = 0; i < NUM_KEYS; i++) {
      buffer.put((byte) sizes[i]);
    }

    for (int i = 0; i < NUM_KEYS; i++) {
      int size = sizes[i];
      if (size > 0) {
        containers[i].serialize(buffer, size);
      }
    }

    buffer.flip();
    return buffer;
  }

  static MumblingBitmap deserialize(byte[] bytes) {
    return deserialize(ByteBuffer.wrap(bytes));
  }

  /** Deserializes a bitmap from the given buffer, advancing its position. */
  public static MumblingBitmap deserialize(ByteBuffer buffer) {
    MumblingBitmap bitmap = new MumblingBitmap();
    for (int i = 0; i < NUM_KEYS; i++) {
      bitmap.sizes[i] = (short) (buffer.get() & 0xFF);
    }

    for (int i = 0; i < NUM_KEYS; i++) {
      int size = bitmap.sizes[i];
      if (size > SPARSE_CAPACITY) {
        long[] words = new long[4];
        for (int w = 0; w < 4; w++) {
          words[w] = buffer.getLong();
        }
        bitmap.containers[i] = new DenseContainer(words);
      } else if (size > 0) {
        SparseContainer sparse = (SparseContainer) bitmap.containers[i];
        buffer.get(sparse.positions, 0, size);
      }
    }

    return bitmap;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof MumblingBitmap)) {
      return false;
    }

    MumblingBitmap that = (MumblingBitmap) other;
    return Arrays.equals(sizes, that.sizes) && Arrays.equals(containers, that.containers);
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(sizes) + Arrays.hashCode(containers);
  }

    private static void checkPosition(int position) {
        if (position < 0 || position >= MAX_POSITION) {
            throw new IllegalArgumentException(String.format("Invalid position: %s", position));
        }
    }

  /** A mini-container holding the low bytes for a single high-byte key. */
  private abstract static class MiniContainer {
    /** Returns whether {@code value} (unsigned 0..255) is contained. */
    abstract boolean contains(int value, int size);

    /** Returns the cardinality given the stored size counter. */
    abstract int cardinality(int count);

    /** Writes this container to the buffer. */
    abstract void serialize(ByteBuffer buffer, int size);
  }

  /** A sorted list of up to 31 distinct low bytes. */
  private static final class SparseContainer extends MiniContainer {
    private final byte[] positions = new byte[SPARSE_CAPACITY];

    @Override
    boolean contains(int value, int size) {
      for (int index = 0; index < size; index++) {
        int current = positions[index] & 0xFF;
        if (value == current) {
          return true;
        } else if (value < current) {
          return false;
        }
      }

      return false;
    }

    @Override
    int cardinality(int count) {
      // a sparse container can never be full, so the count is exact
      return count;
    }

    @Override
    void serialize(ByteBuffer buffer, int size) {
      buffer.put(positions, 0, size);
    }

    /**
     * Inserts {@code value} into the sorted positions, keeping them ordered, and updates the size
     * counter for the given key.
     *
     * @return {@code true} if the value was added, {@code false} if it was already present
     */
    boolean insert(int value, MumblingBitmap bitmap, int key) {
      int count = bitmap.sizes[key];
      if (count >= SPARSE_CAPACITY) {
        throw new IllegalStateException("Cannot add position, already full");
      }

      int insertAt = findInsertIndex(value, count);
      if (insertAt == count || value != (positions[insertAt] & 0xFF)) {
        if (insertAt != count) {
          moveTail(insertAt, count);
        }

        positions[insertAt] = (byte) value;
        bitmap.sizes[key] = (short) (bitmap.sizes[key] + 1);
        return true;
      } else {
        return false;
      }
    }

    private int findInsertIndex(int value, int count) {
      int index = 0;
      while (index < count && value > (positions[index] & 0xFF)) {
        index += 1;
      }

      return index;
    }

    private void moveTail(int index, int count) {
      int pos = count;
      while (pos > index) {
        positions[pos] = positions[pos - 1];
        pos -= 1;
      }
    }

    /** Promotes this sparse container to an equivalent dense container. */
    DenseContainer toDense() {
      long[] words = new long[4];
      for (byte position : positions) {
        DenseContainer.insert(position & 0xFF, words);
      }

      return new DenseContainer(words);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof SparseContainer)) {
        return false;
      }

      return Arrays.equals(positions, ((SparseContainer) other).positions);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(positions);
    }
  }

  /** A 256-bit dense bitmap stored as four 64-bit words. */
  private static final class DenseContainer extends MiniContainer {
    private final long[] words;

    DenseContainer(long[] words) {
      this.words = words;
    }

    @Override
    boolean contains(int value, int size) {
      long pattern = bitPattern(value);
      long word = words[value >>> 6];
      return (word & pattern) == pattern;
    }

    @Override
    int cardinality(int count) {
      if (count < MAX_SIZE) {
        return count;
      } else if (hasUnsetPosition()) {
        return 255;
      } else {
        return 256;
      }
    }

    @Override
    void serialize(ByteBuffer buffer, int size) {
      for (long word : words) {
        buffer.putLong(word);
      }
    }

    /**
     * Sets the bit for {@code value}.
     *
     * @return {@code true} if the bit was newly set, {@code false} if it was already set
     */
    boolean insert(int value) {
      return insert(value, words);
    }

    private static boolean insert(int value, long[] words) {
      long pattern = bitPattern(value);
      int index = value >>> 6;
      boolean added = (words[index] & pattern) != pattern;
      words[index] |= pattern;
      return added;
    }

    /**
     * Returns whether any bit is unset. When a container is full the count cannot represent the
     * actual cardinality, so this distinguishes a count of 255 from 256.
     */
    private boolean hasUnsetPosition() {
      for (long word : words) {
        if (word != -1L) { // -1L == u64::MAX
          return true;
        }
      }

      return false;
    }

    private static long bitPattern(int value) {
      return 1L << (63 - (value & 0x3F));
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof DenseContainer)) {
        return false;
      }

      return Arrays.equals(words, ((DenseContainer) other).words);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(words);
    }
  }
}
