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
package org.apache.iceberg.parquet;

import com.github.luben.zstd.Zstd;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prototype of the ALP encodings for the embeddings of {@link ParquetFloat16EmbeddingBenchmark},
 * compared with dictionary encoding.
 *
 * <p>Estimates the size of the embeddings encoded with ALP and ALP_rd (Afroozeh, Kuffo, Boncz:
 * "ALP: Adaptive Lossless floating-Point Compression", SIGMOD 2024), and with Parquet's dictionary
 * encoding, which is Iceberg's default and the smallest Parquet encoding of FLOAT embeddings:
 *
 * <ul>
 *   <li>dictionary: the float16 values of the embeddings as 32-bit floats, like the benchmark's
 *       FLOAT storage with DICTIONARY encoding. The distinct values are stored as is in a
 *       dictionary, and the values are replaced with their ids in the dictionary, which are
 *       bit-packed with the bit width of the largest id
 *   <li>ALP: the 32-bit floats. Every vector of values is multiplied by the power of 10 that turns
 *       the most values into integers that round trip exactly, and the integers are bit-packed with
 *       frame-of-reference encoding. Values that do not round trip are exceptions, stored as is
 *   <li>ALP_rd, ALP's encoding for values that are not decimals: the 32-bit floats are split into
 *       left bits, encoded with a dictionary of the 8 most common left parts, and right bits, which
 *       are bit-packed. Left parts that are not in the dictionary are exceptions
 *   <li>float16 dictionary: dictionary encoding of the 16 bits of the float16 values, like the
 *       benchmark's FLOAT16 storage with DICTIONARY encoding
 *   <li>float16 ALP_rd: ALP_rd on the 16 bits of the float16 values. This is not part of ALP, which
 *       only encodes 32-bit and 64-bit floats
 * </ul>
 *
 * <p>The encoded bit streams are also compressed with zstd level 3, Parquet's default level.
 *
 * <p>This is a prototype to estimate sizes, not an implementation of the encodings: it does not
 * measure speed, and it does not write Parquet files, so the sizes do not include repetition and
 * definition levels, page headers and other metadata. ALP tries every exponent and factor for every
 * vector instead of sampling, so its sizes are a best case.
 *
 * <p>The embeddings are generated with the model and the seed of the benchmark, so they are the
 * first {@link #embeddingCount} embeddings of the benchmark. The setup encodes them and logs the
 * details of the encodings, and the {@code sizes} benchmark reports the size of every encoding in
 * bits per value as secondary results, like {@code sizes:alpRd}, and of the encoded values
 * compressed with zstd, like {@code sizes:alpRdZstd}. To run it:
 *
 * <pre>
 * ./gradlew :iceberg-data:jmh
 *     -PjmhIncludeRegex=AlpPrototype
 *     -PjmhOutputPath=benchmark/alp-prototype-result.txt
 * </pre>
 *
 * <p>To encode more or fewer embeddings, change the {@code embeddingCount} {@link Param} or pass
 * {@code -p embeddingCount=...} to JMH.
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 0)
@Measurement(iterations = 1)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class AlpPrototype {
  private static final Logger LOG = LoggerFactory.getLogger(AlpPrototype.class);
  // number of values that ALP encodes with the same exponent, factor and bit width
  private static final int VECTOR_SIZE = 1024;
  // largest power of 10 that ALP uses for 32-bit floats
  private static final int MAX_EXPONENT = 10;
  // largest integer that ALP encodes
  private static final float MAX_DIGITS = 9.2e18f;
  // ALP vector header: exponent, factor and bit width, frame of reference, exception count
  private static final int HEADER_BYTE_BITS = 8;
  private static final int FRAME_OF_REFERENCE_BITS = Long.SIZE;
  // bits of an exception count, and of the position of an exception in its vector
  private static final int POSITION_BITS = 16;
  // ALP_rd dictionary of the left parts: 8 entries of up to 16 bits, with 3-bit codes
  private static final int RD_DICTIONARY_SIZE = 8;
  private static final int RD_DICTIONARY_ENTRY_BITS = 16;
  private static final int RD_CODE_BITS = 3;
  private static final int RD_MAX_LEFT_BITS = 16;
  private static final int RD_FLOAT16_MAX_LEFT_BITS = 10;
  // Parquet's RLE/bit-packing hybrid encoding writes up to 63 groups of 8 ids in a bit-packed run
  private static final int MAX_BIT_PACKED_RUN = 63 * 8;
  private static final int ZSTD_LEVEL = 3;
  private static final float[] POWERS_OF_TEN = new float[MAX_EXPONENT + 1];
  private static final float[] INVERSE_POWERS_OF_TEN = new float[MAX_EXPONENT + 1];

  static {
    for (int exponent = 0; exponent <= MAX_EXPONENT; exponent += 1) {
      POWERS_OF_TEN[exponent] = (float) Math.pow(10, exponent);
      INVERSE_POWERS_OF_TEN[exponent] = (float) Math.pow(10, -exponent);
    }
  }

  /** Number of generated embeddings to encode. */
  @Param("2048")
  private int embeddingCount;

  /** Number of dimensions of the generated embeddings. */
  @Param("2560")
  private int dimension;

  private EncodedSize dictionarySize = null;
  private EncodedSize alpSize = null;
  private EncodedSize alpRdSize = null;
  private EncodedSize float16DictionarySize = null;
  private EncodedSize float16AlpRdSize = null;

  @Setup(Level.Trial)
  public void encodeEmbeddings() {
    List<short[]> embeddings =
        ParquetFloat16EmbeddingBenchmark.generate(
            ParquetFloat16EmbeddingBenchmark.embeddingGenerator(dimension), embeddingCount);

    int valueCount = Math.multiplyExact(embeddingCount, dimension);
    float[] floats = new float[valueCount];
    int[] floatBits = new int[valueCount];
    int[] float16Bits = new int[valueCount];
    int index = 0;
    for (short[] embedding : embeddings) {
      for (short bits : embedding) {
        floats[index] = ParquetConversions.float16ToFloat(bits);
        floatBits[index] = Float.floatToRawIntBits(floats[index]);
        float16Bits[index] = Short.toUnsignedInt(bits);
        index += 1;
      }
    }

    StringBuilder report = new StringBuilder();
    report.append(
        String.format(
            Locale.ROOT,
            "%d embeddings with %d dimensions: %d float16 values, in ALP vectors of %d values",
            embeddingCount,
            dimension,
            valueCount,
            VECTOR_SIZE));

    this.dictionarySize = dictionary("dictionary", floatBits, Float.SIZE, report);
    this.alpSize = alp(floats, report);
    this.alpRdSize = alpRd("ALP_rd", floatBits, Float.SIZE, RD_MAX_LEFT_BITS, report);
    this.float16DictionarySize = dictionary("float16 dictionary", float16Bits, Short.SIZE, report);
    this.float16AlpRdSize =
        alpRd("float16 ALP_rd", float16Bits, Short.SIZE, RD_FLOAT16_MAX_LEFT_BITS, report);
    LOG.info("Sizes of the encodings:\n{}", report);
  }

  /**
   * Reports the sizes of the encodings in bits per value as secondary results. The sizes are
   * deterministic, so a single iteration is enough.
   */
  @Benchmark
  public void sizes(BitsPerValue bitsPerValue) {
    bitsPerValue.update(
        dictionarySize, alpSize, alpRdSize, float16DictionarySize, float16AlpRdSize);
  }

  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class BitsPerValue {
    private double scale = 1.0;
    private double dictionary = 0.0;
    private double dictionaryZstd = 0.0;
    private double alp = 0.0;
    private double alpZstd = 0.0;
    private double alpRd = 0.0;
    private double alpRdZstd = 0.0;
    private double float16Dictionary = 0.0;
    private double float16DictionaryZstd = 0.0;
    private double float16AlpRd = 0.0;
    private double float16AlpRdZstd = 0.0;

    /**
     * JMH sums event counters over all measured iterations and forks, so every iteration reports
     * its share of the size.
     */
    @Setup(Level.Trial)
    public void setupScale(BenchmarkParams params) {
      long measuredIterations =
          (long) Math.max(params.getForks(), 1) * params.getMeasurement().getCount();
      this.scale = 1.0 / measuredIterations;
    }

    public double dictionary() {
      return dictionary;
    }

    public double dictionaryZstd() {
      return dictionaryZstd;
    }

    public double alp() {
      return alp;
    }

    public double alpZstd() {
      return alpZstd;
    }

    public double alpRd() {
      return alpRd;
    }

    public double alpRdZstd() {
      return alpRdZstd;
    }

    public double float16Dictionary() {
      return float16Dictionary;
    }

    public double float16DictionaryZstd() {
      return float16DictionaryZstd;
    }

    public double float16AlpRd() {
      return float16AlpRd;
    }

    public double float16AlpRdZstd() {
      return float16AlpRdZstd;
    }

    void update(
        EncodedSize newDictionary,
        EncodedSize newAlp,
        EncodedSize newAlpRd,
        EncodedSize newFloat16Dictionary,
        EncodedSize newFloat16AlpRd) {
      this.dictionary = newDictionary.raw() * scale;
      this.dictionaryZstd = newDictionary.zstd() * scale;
      this.alp = newAlp.raw() * scale;
      this.alpZstd = newAlp.zstd() * scale;
      this.alpRd = newAlpRd.raw() * scale;
      this.alpRdZstd = newAlpRd.zstd() * scale;
      this.float16Dictionary = newFloat16Dictionary.raw() * scale;
      this.float16DictionaryZstd = newFloat16Dictionary.zstd() * scale;
      this.float16AlpRd = newFloat16AlpRd.raw() * scale;
      this.float16AlpRdZstd = newFloat16AlpRd.zstd() * scale;
    }
  }

  /**
   * Encodes the values of {@code bitWidth} bits with Parquet's dictionary encoding.
   *
   * <p>Like Parquet, the dictionary holds the distinct values in the order in which they first
   * appear, stored as is, and the ids of the values are written with the bit width of the largest
   * id, followed by the ids in the RLE/bit-packing hybrid encoding. The values of the embeddings
   * rarely repeat, so the ids are written in bit-packed runs, which Parquet also uses for fewer
   * than 8 repeated ids. Parquet writes the dictionary to its own page, and falls back to plain
   * encoding when the dictionary is larger than the dictionary page size; float16 values have at
   * most 65536 distinct values, so the dictionary is at most 256 KB, which is below the default
   * limit.
   */
  private static EncodedSize dictionary(
      String name, int[] bits, int bitWidth, StringBuilder report) {
    Map<Integer, Integer> ids = Maps.newHashMap();
    int[] entries = new int[bits.length];
    int[] valueIds = new int[bits.length];
    for (int pos = 0; pos < bits.length; pos += 1) {
      Integer id = ids.get(bits[pos]);
      if (id == null) {
        id = ids.size();
        ids.put(bits[pos], id);
        entries[id] = bits[pos];
      }

      valueIds[pos] = id;
    }

    BitWriter out = new BitWriter();
    int entryCount = ids.size();
    for (int id = 0; id < entryCount; id += 1) {
      out.write(Integer.toUnsignedLong(entries[id]), bitWidth);
    }

    int idWidth = bitWidth(entryCount - 1);
    out.write(idWidth, HEADER_BYTE_BITS);
    for (int start = 0; start < valueIds.length; start += MAX_BIT_PACKED_RUN) {
      int end = Math.min(start + MAX_BIT_PACKED_RUN, valueIds.length);
      int groups = (end - start + 7) / 8;
      // run header: the number of groups of 8 ids, and 1 for a bit-packed run
      out.write((groups << 1) | 1, HEADER_BYTE_BITS);
      for (int pos = start; pos < start + groups * 8; pos += 1) {
        out.write(pos < end ? valueIds[pos] : 0, idWidth);
      }
    }

    EncodedSize size =
        appendSizes(
            String.format(Locale.ROOT, "%s (%d-bit ids)", name, idWidth), out, bits.length, report);
    report
        .append('\n')
        .append(
            String.format(
                Locale.ROOT,
                "    distinct values=%d, dictionary=%d bytes",
                entryCount,
                (long) entryCount * bitWidth / Byte.SIZE));
    return size;
  }

  /** Encodes the values with ALP, choosing the best exponent and factor for every vector. */
  private static EncodedSize alp(float[] values, StringBuilder report) {
    BitWriter out = new BitWriter();
    Map<String, Integer> chosen = Maps.newHashMap();
    long exceptionCount = 0;
    long widthSum = 0;
    int maxWidth = 0;
    int vectorCount = 0;
    for (int start = 0; start < values.length; start += VECTOR_SIZE) {
      float[] vector =
          Arrays.copyOfRange(values, start, Math.min(start + VECTOR_SIZE, values.length));

      int[] exponentAndFactor = bestExponentAndFactor(vector);
      int exponent = exponentAndFactor[0];
      int factor = exponentAndFactor[1];
      chosen.merge("e=" + exponent + ",f=" + factor, 1, Integer::sum);

      long[] digits = new long[vector.length];
      boolean[] isException = new boolean[vector.length];
      int exceptions = 0;
      boolean hasFill = false;
      long fill = 0;
      for (int pos = 0; pos < vector.length; pos += 1) {
        long encoded = encode(vector[pos], exponent, factor);
        if (roundTrips(vector[pos], encoded, exponent, factor)) {
          digits[pos] = encoded;
          if (!hasFill) {
            fill = encoded;
            hasFill = true;
          }
        } else {
          isException[pos] = true;
          exceptions += 1;
        }
      }

      // exceptions are replaced with an encoded value, so that they do not widen the bit width
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      for (int pos = 0; pos < vector.length; pos += 1) {
        if (isException[pos]) {
          digits[pos] = fill;
        }

        min = Math.min(min, digits[pos]);
        max = Math.max(max, digits[pos]);
      }

      int width = bitWidth(max - min);
      out.write(exponent, HEADER_BYTE_BITS);
      out.write(factor, HEADER_BYTE_BITS);
      out.write(width, HEADER_BYTE_BITS);
      out.write(min, FRAME_OF_REFERENCE_BITS);
      out.write(exceptions, POSITION_BITS);
      for (long encoded : digits) {
        out.write(encoded - min, width);
      }

      for (int pos = 0; pos < vector.length; pos += 1) {
        if (isException[pos]) {
          out.write(Integer.toUnsignedLong(Float.floatToRawIntBits(vector[pos])), Float.SIZE);
          out.write(pos, POSITION_BITS);
        }
      }

      exceptionCount += exceptions;
      widthSum += width;
      maxWidth = Math.max(maxWidth, width);
      vectorCount += 1;
    }

    EncodedSize size = appendSizes("ALP", out, values.length, report);
    report
        .append('\n')
        .append(
            String.format(
                Locale.ROOT,
                "    avg bit width=%.1f (max %d), exceptions=%.2f%%, most chosen: %s",
                (double) widthSum / vectorCount,
                maxWidth,
                100.0 * exceptionCount / values.length,
                mostCommon(chosen, 3)));
    return size;
  }

  /**
   * Returns the exponent and the factor that encode a vector in the fewest bits.
   *
   * <p>Tries every exponent and factor; ALP samples the values instead, so this is a best case.
   */
  private static int[] bestExponentAndFactor(float[] vector) {
    long bestBits = Long.MAX_VALUE;
    int[] best = new int[2];
    for (int exponent = 0; exponent <= MAX_EXPONENT; exponent += 1) {
      for (int factor = 0; factor <= exponent; factor += 1) {
        long bits = alpVectorBits(vector, exponent, factor);
        if (bits < bestBits) {
          bestBits = bits;
          best[0] = exponent;
          best[1] = factor;
        }
      }
    }

    return best;
  }

  /** Returns the bits of a vector encoded with an exponent and a factor, without the header. */
  private static long alpVectorBits(float[] vector, int exponent, int factor) {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    int exceptions = 0;
    for (float value : vector) {
      long encoded = encode(value, exponent, factor);
      if (roundTrips(value, encoded, exponent, factor)) {
        min = Math.min(min, encoded);
        max = Math.max(max, encoded);
      } else {
        exceptions += 1;
      }
    }

    int width = exceptions == vector.length ? 0 : bitWidth(max - min);
    return (long) width * vector.length + exceptions * (long) (Float.SIZE + POSITION_BITS);
  }

  private static long encode(float value, int exponent, int factor) {
    float scaled = value * POWERS_OF_TEN[exponent] * INVERSE_POWERS_OF_TEN[factor];
    if (!Float.isFinite(scaled) || Math.abs(scaled) > MAX_DIGITS) {
      return 0;
    }

    return Math.round((double) scaled);
  }

  private static boolean roundTrips(float value, long encoded, int exponent, int factor) {
    float decoded = (float) encoded * POWERS_OF_TEN[factor] * INVERSE_POWERS_OF_TEN[exponent];
    return Float.floatToRawIntBits(decoded) == Float.floatToRawIntBits(value);
  }

  /**
   * Encodes the values with ALP_rd, splitting every value of {@code bitWidth} bits into left and
   * right bits.
   *
   * <p>The cut and the dictionary of the most common left parts are chosen once for all values,
   * like ALP_rd does for a row group.
   */
  private static EncodedSize alpRd(
      String name, int[] bits, int bitWidth, int maxLeftBits, StringBuilder report) {
    StringBuilder search = new StringBuilder("   ");
    int leftBits = 0;
    long bestBits = Long.MAX_VALUE;
    for (int candidate = 1; candidate <= maxLeftBits; candidate += 1) {
      long estimate = alpRdBits(bits, bitWidth - candidate, candidate);
      search.append(
          String.format(Locale.ROOT, " %d->%.1f", candidate, (double) estimate / bits.length));
      if (estimate < bestBits) {
        bestBits = estimate;
        leftBits = candidate;
      }
    }

    report.append('\n').append(name + " cut search (left bits -> estimated bits/value):");
    report.append('\n').append(search);

    int rightBits = bitWidth - leftBits;
    int[] dictionary = rdDictionary(leftPartCounts(bits, rightBits));
    Map<Integer, Integer> codes = Maps.newHashMap();
    for (int code = 0; code < dictionary.length; code += 1) {
      codes.put(dictionary[code], code);
    }

    BitWriter out = new BitWriter();
    for (int leftPart : dictionary) {
      out.write(leftPart, RD_DICTIONARY_ENTRY_BITS);
    }

    long rightMask = (1L << rightBits) - 1;
    long exceptionCount = 0;
    for (int start = 0; start < bits.length; start += VECTOR_SIZE) {
      int end = Math.min(start + VECTOR_SIZE, bits.length);
      int exceptions = 0;
      for (int pos = start; pos < end; pos += 1) {
        Integer code = codes.get(bits[pos] >>> rightBits);
        if (code == null) {
          out.write(0, RD_CODE_BITS);
          exceptions += 1;
        } else {
          out.write(code, RD_CODE_BITS);
        }
      }

      for (int pos = start; pos < end; pos += 1) {
        out.write(bits[pos] & rightMask, rightBits);
      }

      out.write(exceptions, POSITION_BITS);
      for (int pos = start; pos < end; pos += 1) {
        int leftPart = bits[pos] >>> rightBits;
        if (!codes.containsKey(leftPart)) {
          out.write(leftPart, leftBits);
          out.write(pos - start, POSITION_BITS);
        }
      }

      exceptionCount += exceptions;
    }

    EncodedSize size =
        appendSizes(
            String.format(
                Locale.ROOT, "%s (left=%d bits, right=%d bits)", name, leftBits, rightBits),
            out,
            bits.length,
            report);
    report
        .append('\n')
        .append(
            String.format(
                Locale.ROOT, "    exceptions=%.2f%%", 100.0 * exceptionCount / bits.length));
    return size;
  }

  /** Estimates the bits of the values encoded with ALP_rd with a cut. */
  private static long alpRdBits(int[] bits, int rightBits, int leftBits) {
    long covered =
        leftPartCounts(bits, rightBits).values().stream()
            .sorted((count1, count2) -> Integer.compare(count2, count1))
            .limit(RD_DICTIONARY_SIZE)
            .mapToLong(Integer::longValue)
            .sum();
    long exceptions = bits.length - covered;
    return (long) bits.length * (rightBits + RD_CODE_BITS)
        + exceptions * (leftBits + POSITION_BITS)
        + (long) RD_DICTIONARY_SIZE * RD_DICTIONARY_ENTRY_BITS;
  }

  private static Map<Integer, Integer> leftPartCounts(int[] bits, int rightBits) {
    Map<Integer, Integer> counts = Maps.newHashMap();
    for (int value : bits) {
      counts.merge(value >>> rightBits, 1, Integer::sum);
    }

    return counts;
  }

  /** Returns the most common left parts. */
  private static int[] rdDictionary(Map<Integer, Integer> leftPartCounts) {
    return leftPartCounts.entrySet().stream()
        .sorted((entry1, entry2) -> Integer.compare(entry2.getValue(), entry1.getValue()))
        .limit(RD_DICTIONARY_SIZE)
        .mapToInt(Map.Entry::getKey)
        .toArray();
  }

  private static int bitWidth(long range) {
    return Long.SIZE - Long.numberOfLeadingZeros(range);
  }

  /** Appends the sizes of the encoded values to the report, and returns them. */
  private static EncodedSize appendSizes(
      String name, BitWriter out, int valueCount, StringBuilder report) {
    byte[] raw = out.toBytes();
    byte[] compressed = Zstd.compress(raw, ZSTD_LEVEL);
    report
        .append('\n')
        .append(
            String.format(
                Locale.ROOT,
                "%-44s raw=%9d bytes (%5.2f bits/value)  zstd=%9d bytes (%5.2f bits/value)",
                name,
                raw.length,
                raw.length * 8.0 / valueCount,
                compressed.length,
                compressed.length * 8.0 / valueCount));
    return new EncodedSize(raw.length * 8.0 / valueCount, compressed.length * 8.0 / valueCount);
  }

  private static String mostCommon(Map<String, Integer> counts, int limit) {
    return counts.entrySet().stream()
        .sorted((entry1, entry2) -> Integer.compare(entry2.getValue(), entry1.getValue()))
        .limit(limit)
        .map(entry -> entry.getKey() + " x" + entry.getValue())
        .collect(Collectors.joining("  "));
  }

  /** Size of encoded values in bits per value, and compressed with zstd. */
  private static final class EncodedSize {
    private final double raw;
    private final double zstd;

    private EncodedSize(double raw, double zstd) {
      this.raw = raw;
      this.zstd = zstd;
    }

    double raw() {
      return raw;
    }

    double zstd() {
      return zstd;
    }
  }

  /** Packs values into a bit stream, least significant bits first. */
  private static final class BitWriter {
    private long[] words = new long[1 << 16];
    private long bitPosition = 0;

    void write(long value, int width) {
      if (width == 0) {
        return;
      }

      long masked = width == Long.SIZE ? value : value & ((1L << width) - 1);
      int word = (int) (bitPosition >>> 6);
      int offset = (int) (bitPosition & 63);
      ensureCapacity(word + 2);
      words[word] |= masked << offset;
      if (offset + width > Long.SIZE) {
        words[word + 1] |= masked >>> (Long.SIZE - offset);
      }

      bitPosition += width;
    }

    private void ensureCapacity(int size) {
      if (size > words.length) {
        this.words = Arrays.copyOf(words, Math.max(size, words.length * 2));
      }
    }

    byte[] toBytes() {
      int byteCount = Math.toIntExact((bitPosition + 7) >>> 3);
      int wordCount = (byteCount + 7) / 8;
      ByteBuffer buffer = ByteBuffer.allocate(wordCount * 8).order(ByteOrder.LITTLE_ENDIAN);
      for (int word = 0; word < wordCount; word += 1) {
        buffer.putLong(words[word]);
      }

      return Arrays.copyOf(buffer.array(), byteCount);
    }
  }
}
