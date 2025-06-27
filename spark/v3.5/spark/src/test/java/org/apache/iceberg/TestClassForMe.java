/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.DateColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.orc.OrcBatchReader;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

public class TestClassForMe {
    private static final Schema SCHEMA =
            new Schema(
                    required(1, "longCol", Types.LongType.get()),
                    required(2, "intCol", Types.IntegerType.get()),
                    required(3, "floatCol", Types.FloatType.get()),
                    optional(4, "doubleCol", Types.DoubleType.get()),
                    optional(5, "decimalCol", Types.DecimalType.of(20, 5)),
                    optional(6, "dateCol", Types.DateType.get()),
                    optional(7, "timestampCol", Types.TimestampType.withZone()),
                    optional(8, "stringCol", Types.StringType.get()));
    private static final int NUM_RECORDS = 1000000;
    private FileIO fileIO;
    private final static String FILE_NAME = "data.orc";

    @BeforeEach
    public void setupBenchmark() throws IOException {
        fileIO = new InMemoryFileIO();

        List<Record> records = RandomGenericData.generate(SCHEMA, NUM_RECORDS, 0L);
        try (FileAppender<Record> writer =
                     ORC.write(fileIO.newOutputFile(FILE_NAME)).createWriterFunc(GenericOrcWriter::buildWriter).schema(SCHEMA).set("orc.compress", "none").build()) {
            writer.addAll(records);
        }
    }

    @Test
    public void readUsingIcebergReaderVectorizedMem() throws IOException {
        try (CloseableIterable<ColumnarBatch> rows =
                     ORC.read(fileIO.newInputFile(FILE_NAME))
                             .project(SCHEMA)
                             .createBatchedReaderFunc(fileSchema ->
                                     VectorizedSparkOrcReaders.buildReader(SCHEMA, fileSchema, Map.of()))
                             .build();
             CloseableIterable<VectorizedRowBatch> rows1 =
                     ORC.read(fileIO.newInputFile(FILE_NAME))
                             .project(SCHEMA)
                         .createBatchedReaderFunc(fileSchema ->
                                 new OrcBatchReader<VectorizedRowBatch>() {
                                   @Override
                                   public VectorizedRowBatch read(VectorizedRowBatch batch) {
                                     return batch;
                                   }

                                   @Override
                                   public void setBatchContext(long batchOffsetInFile) {

                                   }
                                 })
                             .build()) {

            long val = 0;
            long val1 = 0;
            VectorizedRowBatch otherBatch;
            for (ColumnarBatch row : rows) {
                otherBatch = rows1.iterator().next();
                Iterator<InternalRow> rowIterator = row.rowIterator();
                for(int i = 0; i<otherBatch.size; ++i) {
                    InternalRow internalRow = rowIterator.next();
                    BytesColumnVector bytesVector = (BytesColumnVector) (otherBatch.cols[7]);
                    val ^= internalRow.isNullAt(0)? 0 :internalRow.getLong(0);
                    val1 ^= otherBatch.cols[0].isNull[i] ? 0 :((LongColumnVector) otherBatch.cols[0]).vector[i];
                    val ^= internalRow.isNullAt(1)? 0 :internalRow.getInt(1);
                    val1 ^= otherBatch.cols[1].isNull[i] ? 0 :((LongColumnVector) otherBatch.cols[1]).vector[i];
                    val ^= internalRow.isNullAt(2)? 0 :((long) internalRow.getFloat(2));
                    val1 ^= otherBatch.cols[2].isNull[i] ? 0 :(long) ((DoubleColumnVector) otherBatch.cols[2]).vector[i];
                    val ^= internalRow.isNullAt(3)? 0 :((long) internalRow.getDouble(3));
                    val1 ^= otherBatch.cols[3].isNull[i] ? 0 :(long) ((DoubleColumnVector) otherBatch.cols[3]).vector[i];
                    val ^= internalRow.isNullAt(4)? 0 :internalRow.getDecimal(4, 20, 5).toBigDecimal().longValue();
                    val1 ^= otherBatch.cols[4].isNull[i] ? 0 :((DecimalColumnVector) otherBatch.cols[4]).vector[i].longValue();
                    val ^= internalRow.isNullAt(5)? 0 :internalRow.getInt(5);
                    val1 ^= otherBatch.cols[5].isNull[i] ? 0 :((DateColumnVector) otherBatch.cols[5]).vector[i];
                    val ^= internalRow.isNullAt(6)? 0 :internalRow.getLong(6);
                    val1 ^= otherBatch.cols[6].isNull[i] ? 0 :(((TimestampColumnVector) otherBatch.cols[6]).time[i])/1000*1000_000L + ((TimestampColumnVector) otherBatch.cols[6]).nanos[i]/1000L;
                    val ^= internalRow.isNullAt(7)? 0 :internalRow.getString(7).length();
                    val1 ^= otherBatch.cols[7].isNull[i] ? 0 :UTF8String.fromBytes(bytesVector.vector[i], bytesVector.start[i], bytesVector.length[i]).numChars();
                    assertThat(val).as(" " + i).isEqualTo(val1);
                }
                System.err.println("XOR val: " + val + " XOR val1: " + val1);
            }


        }
    }
}
