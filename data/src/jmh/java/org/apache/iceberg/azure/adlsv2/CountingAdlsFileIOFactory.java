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
package org.apache.iceberg.azure.adlsv2;

import com.azure.core.http.HttpClient;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import java.util.Map;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.FileIO;

/**
 * Helper that lives in {@code org.apache.iceberg.azure.adlsv2} so it can use the package-private
 * {@link ADLSLocation} accepted by {@link ADLSFileIO#ADLSFileIO(
 * org.apache.iceberg.util.SerializableFunction)}. Used by the JMH benchmark to attach a counting
 * {@link HttpPipelinePolicy} to the underlying {@code DataLakeFileSystemClient}.
 */
public final class CountingAdlsFileIOFactory {
  private CountingAdlsFileIOFactory() {}

  public static FileIO create(Map<String, String> props, HttpPipelinePolicy policy) {
    AzureProperties azureProps = new AzureProperties(props);
    ADLSFileIO fileIO =
        new ADLSFileIO(
            location -> {
              DataLakeFileSystemClientBuilder builder =
                  new DataLakeFileSystemClientBuilder().httpClient(HttpClient.createDefault());
              location.container().ifPresent(builder::fileSystemName);
              azureProps.applyClientConfiguration(location.host(), builder);
              builder.addPolicy(policy);
              return builder.buildClient();
            });
    // Re-initialize so the FileIO picks up adls.read.block-size-bytes / adls.lazy-open / etc.
    fileIO.initialize(props);
    return fileIO;
  }
}

