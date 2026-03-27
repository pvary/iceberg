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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.Pair;

public class NarrowedRecord implements Record, StructLike {
  // Cache for position mappings: posMapping[narrowedPos] = originalPos
  private static final LoadingCache<Pair<StructType, StructType>, int[]> CACHE =
      Caffeine.newBuilder()
          .weakKeys()
          .build(
              key -> {
                List<Types.NestedField> projectedFields = key.second().fields();
                List<Types.NestedField> originalFields = key.first().fields();
                int[] posMapping = new int[projectedFields.size()];

                for (int narrowedPos = 0; narrowedPos < projectedFields.size(); narrowedPos++) {
                  Types.NestedField projectedField = projectedFields.get(narrowedPos);
                  for (int originalPos = 0; originalPos < originalFields.size(); originalPos++) {
                    if (originalFields.get(originalPos).fieldId() == projectedField.fieldId()) {
                      posMapping[narrowedPos] = originalPos;
                      break;
                    }
                  }
                }

                return posMapping;
              });

  public static NarrowedRecord create(Schema schema, Integer[] family) {
    return new NarrowedRecord(schema.asStruct(), family);
  }

  private final StructType struct;
  private final int size;
  private final int[] posMapping;
  private Record wrappedRecord;

  private NarrowedRecord(StructType original, Integer[] family) {
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(family.length);
    for (Integer fieldId : family) {
      Types.NestedField field = original.field(fieldId);
      Preconditions.checkArgument(field != null, "Cannot find field with id: %s", fieldId);
      fields.add(field);
    }

    this.struct = StructType.of(fields);
    this.size = struct.fields().size();
    this.posMapping = CACHE.get(Pair.of(original, struct));
  }

  public void set(Record newValue) {
    this.wrappedRecord = newValue;
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
        return wrappedRecord.get(posMapping[i]);
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
        wrappedRecord.set(posMapping[i], value);
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
    return wrappedRecord.get(posMapping[pos]);
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
    wrappedRecord.set(posMapping[pos], value);
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
    sb.append("Record(");
    for (int i = 0; i < struct.fields().size(); i += 1) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(get(i));
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof NarrowedRecord)) {
      return false;
    }

    NarrowedRecord that = (NarrowedRecord) other;
    return Objects.equal(this.wrappedRecord, that.wrappedRecord);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(wrappedRecord);
  }
}
