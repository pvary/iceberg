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
package org.apache.iceberg.vortex;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import dev.vortex.api.Expression;
import dev.vortex.api.expressions.Binary;
import dev.vortex.api.expressions.GetItem;
import dev.vortex.api.expressions.Literal;
import dev.vortex.api.expressions.Not;
import dev.vortex.api.expressions.Root;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestConvertFilterToVortex {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "name", Types.StringType.get()),
          optional(3, "salary", Types.LongType.get()));

  @Test
  public void testIn() {
    Expression result = ConvertFilterToVortex.convert(SCHEMA, Expressions.in("id", 1L, 2L, 3L));
    GetItem field = GetItem.of(Root.INSTANCE, "id");
    Expression expected =
        Binary.or(
            Binary.eq(field, Literal.int64(1L)),
            Binary.eq(field, Literal.int64(2L)),
            Binary.eq(field, Literal.int64(3L)));
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void testNotIn() {
    Expression result = ConvertFilterToVortex.convert(SCHEMA, Expressions.notIn("id", 1L, 2L, 3L));
    GetItem field = GetItem.of(Root.INSTANCE, "id");
    Expression expected =
        Not.of(
            Binary.or(
                Binary.eq(field, Literal.int64(1L)),
                Binary.eq(field, Literal.int64(2L)),
                Binary.eq(field, Literal.int64(3L))));
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void testInSingleValue() {
    // A single-value IN is optimized by Iceberg to EQ during binding
    Expression result = ConvertFilterToVortex.convert(SCHEMA, Expressions.in("id", 42L));
    GetItem field = GetItem.of(Root.INSTANCE, "id");
    Expression expected = Binary.eq(field, Literal.int64(42L));
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void testInStrings() {
    Expression result =
        ConvertFilterToVortex.convert(SCHEMA, Expressions.in("name", "Alice", "Bob"));
    GetItem field = GetItem.of(Root.INSTANCE, "name");
    Expression expected =
        Binary.or(
            Binary.eq(field, Literal.string("Alice")), Binary.eq(field, Literal.string("Bob")));
    assertThat(result).isEqualTo(expected);
  }
}
