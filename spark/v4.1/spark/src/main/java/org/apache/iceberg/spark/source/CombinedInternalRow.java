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
package org.apache.iceberg.spark.source;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.Arrays;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.GeographyVal;
import org.apache.spark.unsafe.types.GeometryVal;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

/**
 * An {@link InternalRow} implementation that combines multiple InternalRow instances, each
 * containing a subset of columns. This is similar to {@link org.apache.iceberg.data.CombinedRecord}
 * but for Spark's InternalRow.
 */
public class CombinedInternalRow extends InternalRow {
  // Cache to address the column values based on the field position.
  private static final LoadingCache<
          Pair<Types.StructType, Integer[][]>, Map<Integer, Pair<Integer, Integer>>>
      COMBINER_CACHE =
          Caffeine.newBuilder()
              .weakKeys()
              .build(
                  key -> {
                    Map<Integer, Pair<Integer, Integer>> posToInternalPos = Maps.newHashMap();

                    // Populate the map with field positions and their corresponding row and field
                    // positions.
                    for (int rowId = 0; rowId < key.second().length; rowId += 1) {
                      for (int rowFieldPos = 0;
                          rowFieldPos < key.second()[rowId].length;
                          rowFieldPos += 1) {
                        int fieldId = key.second()[rowId][rowFieldPos];
                        // Find the position of this field in the schema
                        for (int fieldPos = 0;
                            fieldPos < key.first().fields().size();
                            fieldPos += 1) {
                          if (key.first().fields().get(fieldPos).fieldId() == fieldId) {
                            posToInternalPos.put(fieldPos, Pair.of(rowId, rowFieldPos));
                            break;
                          }
                        }
                      }
                    }

                    return posToInternalPos;
                  });

  public static CombinedInternalRow create(Schema schema, Integer[]... families) {
    return new CombinedInternalRow(schema.asStruct(), families);
  }

  public static CombinedInternalRow clone(CombinedInternalRow toClone) {
    return new CombinedInternalRow(toClone);
  }

  private final Types.StructType struct;
  private final Integer[][] columnSplits;
  private final int size;
  private final Map<Integer, Pair<Integer, Integer>> posToInternalPos;
  private final InternalRow[] values;

  private CombinedInternalRow(CombinedInternalRow toClone) {
    this.struct = toClone.struct;
    this.columnSplits = toClone.columnSplits;
    this.size = toClone.size;
    this.posToInternalPos = toClone.posToInternalPos;
    this.values = new InternalRow[columnSplits.length];
  }

  private CombinedInternalRow(Types.StructType struct, Integer[][] columnSplits) {
    this.struct = struct;
    this.columnSplits = columnSplits;
    this.size = struct.fields().size();
    this.posToInternalPos = COMBINER_CACHE.get(Pair.of(struct, columnSplits));
    this.values = new InternalRow[columnSplits.length];
  }

  public void setColumnSplit(int rowPos, InternalRow value) {
    Preconditions.checkArgument(
        rowPos >= 0 && rowPos < columnSplits.length,
        "Position out of bounds: %s (size: %s)",
        rowPos,
        columnSplits.length);
    Preconditions.checkArgument(
        value.numFields() >= columnSplits[rowPos].length,
        "Cannot set value with %s fields at position %s, expected minimal size is %s",
        value.numFields(),
        rowPos,
        columnSplits[rowPos].length);
    values[rowPos] = value;
  }

  @Override
  public int numFields() {
    return size;
  }

  @Override
  public void setNullAt(int ordinal) {
    throw new UnsupportedOperationException("CombinedInternalRow is read-only");
  }

  @Override
  public void update(int ordinal, Object value) {
    throw new UnsupportedOperationException("CombinedInternalRow is read-only");
  }

  @Override
  public InternalRow copy() {
    // Copy all internal rows and create a new combined row
    CombinedInternalRow copy = new CombinedInternalRow(this);
    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        copy.values[i] = values[i].copy();
      }
    }
    return copy;
  }

  @Override
  public boolean isNullAt(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    if (internalPos == null) {
      return true;
    }
    return values[internalPos.first()].isNullAt(internalPos.second());
  }

  @Override
  public boolean getBoolean(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getBoolean(internalPos.second());
  }

  @Override
  public byte getByte(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getByte(internalPos.second());
  }

  @Override
  public short getShort(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getShort(internalPos.second());
  }

  @Override
  public int getInt(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getInt(internalPos.second());
  }

  @Override
  public long getLong(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getLong(internalPos.second());
  }

  @Override
  public float getFloat(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getFloat(internalPos.second());
  }

  @Override
  public double getDouble(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getDouble(internalPos.second());
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getDecimal(internalPos.second(), precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getUTF8String(internalPos.second());
  }

  @Override
  public byte[] getBinary(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getBinary(internalPos.second());
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getInterval(internalPos.second());
  }

  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getStruct(internalPos.second(), numFields);
  }

  @Override
  public ArrayData getArray(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getArray(internalPos.second());
  }

  @Override
  public MapData getMap(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getMap(internalPos.second());
  }

  @Override
  public VariantVal getVariant(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getVariant(internalPos.second());
  }

  @Override
  public GeographyVal getGeography(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getGeography(internalPos.second());
  }

  @Override
  public GeometryVal getGeometry(int ordinal) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].getGeometry(internalPos.second());
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    Pair<Integer, Integer> internalPos = posToInternalPos.get(ordinal);
    return values[internalPos.first()].get(internalPos.second(), dataType);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("CombinedInternalRow(");
    for (int i = 0; i < values.length; i += 1) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(values[i]);
    }

    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof CombinedInternalRow)) {
      return false;
    }

    CombinedInternalRow that = (CombinedInternalRow) other;
    return Arrays.deepEquals(this.values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode((Object[]) values);
  }
}
