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
import java.util.Objects;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
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

  /**
   * Pre-computed position mapping for O(1) field access. For a field at schema position {@code
   * ordinal}: {@code familyIndex[ordinal]} gives the index into the {@code values} array, and
   * {@code posInFamily[ordinal]} gives the field position within that family's InternalRow.
   */
  private record PositionMapping(int[] familyIndex, int[] posInFamily) {}

  // Cache to address the column values based on the field position.
  private static final LoadingCache<Pair<Types.StructType, Integer[][]>, PositionMapping>
      COMBINER_CACHE =
          Caffeine.newBuilder()
              .weakKeys()
              .build(
                  key -> {
                    int numFields = key.first().fields().size();
                    int[] familyIndex = new int[numFields];
                    int[] posInFamily = new int[numFields];
                    Arrays.fill(familyIndex, -1);

                    for (int rowId = 0; rowId < key.second().length; rowId++) {
                      for (int rowFieldPos = 0;
                          rowFieldPos < key.second()[rowId].length;
                          rowFieldPos++) {
                        int fieldId = key.second()[rowId][rowFieldPos];
                        for (int fieldPos = 0; fieldPos < numFields; fieldPos++) {
                          if (key.first().fields().get(fieldPos).fieldId() == fieldId) {
                            familyIndex[fieldPos] = rowId;
                            posInFamily[fieldPos] = rowFieldPos;
                            break;
                          }
                        }
                      }
                    }

                    return new PositionMapping(familyIndex, posInFamily);
                  });

  public static CombinedInternalRow create(
      Schema schema, Integer[][] families, boolean reuseContainers) {
    return new CombinedInternalRow(schema.asStruct(), families, reuseContainers);
  }

  public static CombinedInternalRow clone(CombinedInternalRow toClone) {
    return new CombinedInternalRow(toClone);
  }

  private final Types.StructType struct;
  private final Integer[][] columnSplits;
  private final int size;
  private final PositionMapping mapping;
  private final InternalRow[] values;
  private final boolean reuseContainers;

  private CombinedInternalRow(CombinedInternalRow toClone) {
    this.struct = toClone.struct;
    this.columnSplits = toClone.columnSplits;
    this.size = toClone.size;
    this.mapping = toClone.mapping;
    this.reuseContainers = toClone.reuseContainers;
    this.values = new InternalRow[columnSplits.length];
    if (reuseContainers) {
      for (int i = 0; i < columnSplits.length; i++) {
        this.values[i] = new GenericInternalRow(columnSplits[i].length);
      }
    }
  }

  private CombinedInternalRow(
      Types.StructType struct, Integer[][] columnSplits, boolean reuseContainers) {
    this.struct = struct;
    this.columnSplits = columnSplits;
    this.size = struct.fields().size();
    this.mapping = COMBINER_CACHE.get(Pair.of(struct, columnSplits));
    this.reuseContainers = reuseContainers;
    this.values = new InternalRow[columnSplits.length];
    if (reuseContainers) {
      for (int i = 0; i < columnSplits.length; i++) {
        this.values[i] = new GenericInternalRow(columnSplits[i].length);
      }
    }
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
    if (reuseContainers && value instanceof GenericInternalRow source) {
      // Fast path: single native memcpy of the values array into our pre-allocated row.
      // This avoids storing a reference to a potentially reused container.
      System.arraycopy(
          source.values(),
          0,
          ((GenericInternalRow) values[rowPos]).values(),
          0,
          columnSplits[rowPos].length);
    } else {
      // No reuse: the underlying reader returns fresh objects, so storing the reference is safe.
      values[rowPos] = value;
    }
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
    CombinedInternalRow copy = new CombinedInternalRow(this);
    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        if (reuseContainers) {
          // Our owned GenericInternalRow slots — arraycopy into the clone's pre-allocated slots
          System.arraycopy(
              ((GenericInternalRow) values[i]).values(),
              0,
              ((GenericInternalRow) copy.values[i]).values(),
              0,
              columnSplits[i].length);
        } else {
          // Stored references — delegate to each row's own copy
          copy.values[i] = values[i].copy();
        }
      }
    }
    return copy;
  }

  @Override
  public boolean isNullAt(int ordinal) {
    if (mapping.familyIndex[ordinal] < 0) {
      return true;
    }
    return values[mapping.familyIndex[ordinal]].isNullAt(mapping.posInFamily[ordinal]);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return values[mapping.familyIndex[ordinal]].getBoolean(mapping.posInFamily[ordinal]);
  }

  @Override
  public byte getByte(int ordinal) {
    return values[mapping.familyIndex[ordinal]].getByte(mapping.posInFamily[ordinal]);
  }

  @Override
  public short getShort(int ordinal) {
    return values[mapping.familyIndex[ordinal]].getShort(mapping.posInFamily[ordinal]);
  }

  @Override
  public int getInt(int ordinal) {
    return values[mapping.familyIndex[ordinal]].getInt(mapping.posInFamily[ordinal]);
  }

  @Override
  public long getLong(int ordinal) {
    return values[mapping.familyIndex[ordinal]].getLong(mapping.posInFamily[ordinal]);
  }

  @Override
  public float getFloat(int ordinal) {
    return values[mapping.familyIndex[ordinal]].getFloat(mapping.posInFamily[ordinal]);
  }

  @Override
  public double getDouble(int ordinal) {
    return values[mapping.familyIndex[ordinal]].getDouble(mapping.posInFamily[ordinal]);
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return values[mapping.familyIndex[ordinal]].getDecimal(
        mapping.posInFamily[ordinal], precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    return values[mapping.familyIndex[ordinal]].getUTF8String(mapping.posInFamily[ordinal]);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return values[mapping.familyIndex[ordinal]].getBinary(mapping.posInFamily[ordinal]);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return values[mapping.familyIndex[ordinal]].getInterval(mapping.posInFamily[ordinal]);
  }

  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    return values[mapping.familyIndex[ordinal]].getStruct(mapping.posInFamily[ordinal], numFields);
  }

  @Override
  public ArrayData getArray(int ordinal) {
    return values[mapping.familyIndex[ordinal]].getArray(mapping.posInFamily[ordinal]);
  }

  @Override
  public MapData getMap(int ordinal) {
    return values[mapping.familyIndex[ordinal]].getMap(mapping.posInFamily[ordinal]);
  }

  @Override
  public VariantVal getVariant(int ordinal) {
    return values[mapping.familyIndex[ordinal]].getVariant(mapping.posInFamily[ordinal]);
  }

  @Override
  public GeographyVal getGeography(int ordinal) {
    return values[mapping.familyIndex[ordinal]].getGeography(mapping.posInFamily[ordinal]);
  }

  @Override
  public GeometryVal getGeometry(int ordinal) {
    return values[mapping.familyIndex[ordinal]].getGeometry(mapping.posInFamily[ordinal]);
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    return values[mapping.familyIndex[ordinal]].get(mapping.posInFamily[ordinal], dataType);
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
