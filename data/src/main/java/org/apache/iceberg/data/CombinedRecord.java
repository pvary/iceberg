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
package org.apache.iceberg.data;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.Arrays;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.Pair;

public class CombinedRecord implements Record, StructLike {
  // Cache for position mappings: familyIndex[pos] and posInFamily[pos] for fast positional lookups.
  private static final LoadingCache<Pair<StructType, Integer[][]>, PositionMapping> CACHE =
      Caffeine.newBuilder()
          .weakKeys()
          .build(
              key -> {
                StructType structType = key.first();
                Integer[][] fams = key.second();
                int numFields = structType.fields().size();
                int[] familyIndex = new int[numFields];
                int[] posInFamily = new int[numFields];

                // Build a lookup from field name to (familyIdx, posWithinFamily)
                Map<String, int[]> nameToInternalPos = Maps.newHashMapWithExpectedSize(numFields);
                for (int famIdx = 0; famIdx < fams.length; famIdx++) {
                  for (int famFieldPos = 0; famFieldPos < fams[famIdx].length; famFieldPos++) {
                    Types.NestedField field = structType.field(fams[famIdx][famFieldPos]);
                    nameToInternalPos.put(field.name(), new int[] {famIdx, famFieldPos});
                  }
                }

                // Map each field position to the corresponding family and position within family
                for (int fieldPos = 0; fieldPos < numFields; fieldPos++) {
                  int[] internal = nameToInternalPos.get(structType.fields().get(fieldPos).name());
                  familyIndex[fieldPos] = internal[0];
                  posInFamily[fieldPos] = internal[1];
                }

                return new PositionMapping(familyIndex, posInFamily);
              });

  public static CombinedRecord create(Schema schema, Integer[]... families) {
    return new CombinedRecord(schema.asStruct(), families);
  }

  public static CombinedRecord clone(CombinedRecord toClone) {
    return new CombinedRecord(toClone);
  }

  private final StructType struct;
  private final Integer[][] families;
  private final int size;
  private final PositionMapping mapping;
  private final Record[] values;

  private CombinedRecord(CombinedRecord toClone) {
    this.struct = toClone.struct;
    this.families = toClone.families;
    this.size = toClone.size;
    this.mapping = toClone.mapping;
    this.values = new Record[families.length];
  }

  private CombinedRecord(StructType struct, Integer[][] families) {
    this.struct = struct;
    this.families = families;
    this.size = struct.fields().size();
    this.mapping = CACHE.get(Pair.of(struct, families));
    this.values = new Record[families.length];
  }

  public void setFamily(int recordPos, Record value) {
    Preconditions.checkArgument(
        recordPos >= 0 && recordPos < families.length,
        "Position out of bounds: %s (size: %s)",
        recordPos,
        families.length);
    Preconditions.checkArgument(
        value.struct().fields().size() >= families[recordPos].length,
        "Cannot set value with struct %s at position %s, expected minimal size is %s",
        value.struct(),
        recordPos,
        families[recordPos].length);
    values[recordPos] = value;
  }

  @Override
  public StructType struct() {
    return struct;
  }

  @Override
  public Object getField(String name) {
    // Not on the hot path since this is only used for debugging and to support field access by
    // name, which is not expected to be common. If this becomes a bottleneck, we can add a
    // name-to-position mapping cache.
    for (int i = 0; i < size; i++) {
      if (struct.fields().get(i).name().equals(name)) {
        return values[mapping.familyIndex[i]].get(mapping.posInFamily[i]);
      }
    }

    return null;
  }

  @Override
  public void setField(String name, Object value) {
    // Not on the hot path since this is only used for debugging and to support field access by
    // name, which is not expected to be common. If this becomes a bottleneck, we can add a
    // name-to-position mapping cache.
    for (int i = 0; i < size; i++) {
      if (struct.fields().get(i).name().equals(name)) {
        values[mapping.familyIndex[i]].set(mapping.posInFamily[i], value);
        return;
      }
    }

    throw new IllegalArgumentException("Cannot set unknown field named: " + name);
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Object get(int pos) {
    return values[mapping.familyIndex[pos]].get(mapping.posInFamily[pos]);
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    Object value = get(pos);
    if (value == null || javaClass.isInstance(value)) {
      return javaClass.cast(value);
    } else {
      throw new IllegalStateException("Not an instance of " + javaClass.getName() + ": " + value);
    }
  }

  @Override
  public <T> void set(int pos, T value) {
    values[mapping.familyIndex[pos]].set(mapping.posInFamily[pos], value);
  }

  @Override
  public Record copy() {
    return copy(ImmutableMap.of());
  }

  @Override
  public Record copy(Map<String, Object> overwriteValues) {
    GenericRecord copy = GenericRecord.create(this.struct);
    for (int i = 0; i < struct.fields().size(); i += 1) {
      Object overwriteValue = overwriteValues.get(struct.fields().get(i).name());
      copy.set(i, overwriteValue != null ? overwriteValue : get(i));
    }

    return copy;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("CombinedRecord(");
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
    } else if (!(other instanceof CombinedRecord)) {
      return false;
    }

    CombinedRecord that = (CombinedRecord) other;
    return Arrays.deepEquals(this.values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode((Object[]) values);
  }

  private static class PositionMapping {
    final int[] familyIndex;
    final int[] posInFamily;

    PositionMapping(int[] familyIndex, int[] posInFamily) {
      this.familyIndex = familyIndex;
      this.posInFamily = posInFamily;
    }
  }
}
