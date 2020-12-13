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

package org.apache.iceberg;

import java.io.Serializable;
import org.apache.iceberg.io.FileIO;

public interface HasStaticStub {
  Stub stub();

  class Builder {
    private final Stub stub;
    private FileIO io;

    public Builder(Stub stub) {
      this.stub = stub;
    }

    public Builder io(FileIO builderIo) {
      this.io = builderIo;
      return this;
    }

    public Table build() {
      TableOperations ops = new StaticTableOperations(stub.baseTableName, io);
      if (stub.type != null) {
        return MetadataTableUtils.createMetadataTableInstance(ops, stub.baseTableName, stub.metadataTableName,
            stub.type);
      } else {
        return new BaseTable(ops, stub.baseTableName);
      }
    }
  }

  class Stub implements Serializable {
    private final String baseTableName;
    private final String metadataTableName;
    private final MetadataTableType type;

    public Stub(String metadataFileLocation) {
      this.baseTableName = metadataFileLocation;
      this.metadataTableName = null;
      this.type = null;
    }

    public Stub(String baseTableName, String metadataTableName, MetadataTableType type) {
      this.baseTableName = baseTableName;
      this.metadataTableName = metadataTableName;
      this.type = type;
    }
  }
}
