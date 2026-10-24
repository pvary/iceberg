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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.Float16LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

class ParquetConversions {
  private ParquetConversions() {}

  static boolean isFloat16(PrimitiveType parquetType) {
    return parquetType.getLogicalTypeAnnotation() instanceof Float16LogicalTypeAnnotation;
  }

  /**
   * Converts a Parquet FLOAT16 value, stored as a 2-byte little-endian IEEE 754 half-precision
   * float, to a single-precision float. The conversion is exact for every float16 value.
   *
   * @param binary a 2-byte little-endian half-precision float
   * @return the equivalent single-precision float
   */
  static float float16ToFloat(Binary binary) {
    return float16ToFloat(binary.get2BytesLittleEndian());
  }

  /**
   * Converts the bits of an IEEE 754 half-precision float to a single-precision float. The
   * conversion is exact for every float16 value, including subnormals, infinities, and NaN.
   *
   * @param float16Bits the bits of a half-precision float
   * @return the equivalent single-precision float
   */
  static float float16ToFloat(short float16Bits) {
    int sign = (float16Bits & 0x8000) << 16;
    int magnitude = float16Bits & 0x7FFF;
    if (magnitude >= 0x0400 && magnitude < 0x7C00) {
      // normal: rebias the exponent from 15 to 127, (127 - 15) << 10 = 0x1C000, and widen the
      // significand from 10 to 23 bits
      return Float.intBitsToFloat(sign | ((magnitude + 0x1C000) << 13));
    } else if (magnitude >= 0x7C00) {
      // infinity or NaN, keeping the NaN significand bits
      return Float.intBitsToFloat(sign | 0x7F800000 | ((magnitude & 0x03FF) << 13));
    }

    // zero or subnormal: significand * 2^-24, which is exact in single precision
    return Float.intBitsToFloat(sign | Float.floatToRawIntBits(magnitude * 0x1.0p-24f));
  }

  /**
   * Converts a single-precision float to the bits of the nearest IEEE 754 half-precision float,
   * rounding half to even. Values beyond the float16 range become infinities, and NaN stays NaN.
   *
   * @param value a single-precision float
   * @return the bits of the nearest half-precision float
   */
  static short floatToFloat16(float value) {
    int bits = Float.floatToRawIntBits(value);
    int sign = (bits >>> 16) & 0x8000;
    int exponent = (bits >>> 23) & 0xFF;
    int significand = bits & 0x7FFFFF;
    if (exponent == 0xFF) {
      // infinity or NaN; NaN keeps the high significand bits and is quiet, so it stays NaN
      int nanBits = significand != 0 ? 0x0200 | (significand >>> 13) : 0;
      return (short) (sign | 0x7C00 | nanBits);
    }

    int float16Exponent = exponent - 127 + 15;
    if (float16Exponent >= 0x1F) {
      return (short) (sign | 0x7C00); // overflow to infinity
    } else if (float16Exponent > 0) {
      // normal; rounding up may carry into the exponent, up to infinity, which is correct
      return (short) (sign | roundHalfEven((float16Exponent << 23) | significand, 13));
    } else if (float16Exponent >= -10) {
      // subnormal: shift the significand, with its implicit bit, to units of 2^-24
      return (short) (sign | roundHalfEven(significand | 0x800000, 14 - float16Exponent));
    }

    // less than half of the smallest subnormal, 2^-24
    return (short) sign;
  }

  private static int roundHalfEven(int value, int shift) {
    int result = value >>> shift;
    int remainder = value & ((1 << shift) - 1);
    int half = 1 << (shift - 1);
    if (remainder > half || (remainder == half && (result & 1) != 0)) {
      return result + 1;
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  static <T> T convertValue(Type type, PrimitiveType parquetType, Object value) {
    switch (type.typeId()) {
      case FLOAT:
        if (isFloat16(parquetType)) {
          return (T) Float.valueOf(float16ToFloat((Binary) value));
        }

        return (T) value;
      case DOUBLE:
        if (isFloat16(parquetType)) {
          return (T) Double.valueOf(float16ToFloat((Binary) value));
        }

        return (T) value;
      case BOOLEAN:
      case INTEGER:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_NANO:
      case LONG:
        return (T) value;
      case STRING:
        return (T) ((Binary) value).toStringUsingUTF8();
      case UUID:
        return (T) UUIDUtil.convert(((Binary) value).toByteBuffer());
      case FIXED:
      case BINARY:
        return (T) ((Binary) value).toByteBuffer();
      case DECIMAL:
        int scale =
            ((DecimalLogicalTypeAnnotation) parquetType.getLogicalTypeAnnotation()).getScale();
        switch (parquetType.getPrimitiveTypeName()) {
          case INT32:
          case INT64:
            return (T) BigDecimal.valueOf(((Number) value).longValue(), scale);
          case FIXED_LEN_BYTE_ARRAY:
          case BINARY:
            return (T) new BigDecimal(new BigInteger(((Binary) value).getBytes()), scale);
          default:
            throw new IllegalArgumentException(
                "Unsupported primitive type for decimal: " + parquetType.getPrimitiveTypeName());
        }
      default:
        throw new IllegalArgumentException("Unsupported primitive type: " + type);
    }
  }

  static Function<Object, Object> converterFromParquet(
      PrimitiveType parquetType, Type icebergType) {
    Function<Object, Object> fromParquet = converterFromParquet(parquetType);
    if (icebergType != null) {
      if (icebergType.typeId() == Type.TypeID.LONG
          && parquetType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32) {
        return value -> ((Integer) fromParquet.apply(value)).longValue();
      } else if (icebergType.typeId() == Type.TypeID.DOUBLE
          && (parquetType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.FLOAT
              || isFloat16(parquetType))) {
        return value -> ((Float) fromParquet.apply(value)).doubleValue();
      } else if (icebergType.typeId() == Type.TypeID.FIXED && isFloat16(parquetType)) {
        // float16 columns were converted to fixed[2] before float16 support was added
        return binary -> ByteBuffer.wrap(((Binary) binary).getBytes());
      } else if (icebergType.typeId() == Type.TypeID.UUID) {
        return binary -> UUIDUtil.convert(((Binary) binary).toByteBuffer());
      }
    }

    return fromParquet;
  }

  static Function<Object, Object> converterFromParquet(PrimitiveType type) {
    if (type.getOriginalType() != null) {
      switch (type.getOriginalType()) {
        case UTF8:
        case ENUM:
        case JSON:
          // decode to CharSequence to avoid copying into a new String
          return binary -> StandardCharsets.UTF_8.decode(((Binary) binary).toByteBuffer());
        case DECIMAL:
          DecimalLogicalTypeAnnotation decimal =
              (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
          int scale = decimal.getScale();
          switch (type.getPrimitiveTypeName()) {
            case INT32:
            case INT64:
              return num -> BigDecimal.valueOf(((Number) num).longValue(), scale);
            case FIXED_LEN_BYTE_ARRAY:
            case BINARY:
              return bin -> new BigDecimal(new BigInteger(((Binary) bin).getBytes()), scale);
            default:
              throw new IllegalArgumentException(
                  "Unsupported primitive type for decimal: " + type.getPrimitiveTypeName());
          }
        default:
      }
    }

    if (isFloat16(type)) {
      return binary -> float16ToFloat((Binary) binary);
    }

    switch (type.getPrimitiveTypeName()) {
      case FIXED_LEN_BYTE_ARRAY:
      case BINARY:
        return binary -> ByteBuffer.wrap(((Binary) binary).getBytes());
      case INT96:
        return binary ->
            ParquetUtil.extractTimestampInt96(
                ByteBuffer.wrap(((Binary) binary).getBytes()).order(ByteOrder.LITTLE_ENDIAN));
      default:
    }

    return obj -> obj;
  }
}
