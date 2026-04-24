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

import java.net.URI;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared utilities for resolving file URIs and cloud credentials for Vortex file access. */
final class VortexFileUtil {
  private static final Logger LOG = LoggerFactory.getLogger(VortexFileUtil.class);

  private static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";
  private static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";
  private static final String FS_S3A_SESSION_TOKEN = "fs.s3a.session.token";
  private static final String FS_S3A_ENDPOINT = "fs.s3a.endpoint";
  private static final String FS_S3A_ENDPOINT_REGION = "fs.s3a.endpoint.region";
  private static final String ACCESS_KEY_PREFIX = "fs.azure.account.key";
  private static final String FIXED_TOKEN_PREFIX = "fs.azure.sas.fixed.token.";
  // Custom (non-Hadoop-standard) keys understood by this util to bridge to Vortex's native Azure
  // backend when no static credential is available -- e.g. when the user is logged in via
  // `az login` and wants Vortex to reuse those credentials.
  private static final String USE_AZURE_CLI_KEY = "fs.azure.use.azure.cli";
  private static final String BEARER_TOKEN_PREFIX = "fs.azure.account.oauth2.access.token";

  private VortexFileUtil() {}

  static String resolveUri(String location) {
    URI uri = URI.create(location);
    if (uri.getScheme() == null) {
      return Paths.get(location).toAbsolutePath().toUri().toString();
    }
    return uri.toString();
  }

  /**
   * Prefix for JVM system properties that are forwarded verbatim (with the prefix stripped) to the
   * underlying Vortex/object_store backend. Useful for credentials that don't have a natural home
   * in a Hadoop {@code Configuration}, e.g. asking the Rust object_store to use the Azure CLI:
   *
   * <pre>-Dvortex.storage.azure_storage_use_azure_cli=true</pre>
   *
   * Properties supplied this way win over anything derived from the FileIO/Hadoop conf.
   */
  private static final String SYSPROP_PREFIX = "vortex.storage.";

  static Map<String, String> resolveOutputProperties(OutputFile outputFile) {
    URI uri = URI.create(outputFile.location());
    if (uri.getScheme() == null) {
      return overlayWithSysProps(Map.of());
    }

    Map<String, String> base =
        outputFile instanceof HadoopOutputFile hof
            ? resolvePropertiesFromConf(uri, hof.getConf())
            : Map.of();
    return overlayWithSysProps(base);
  }

  static Map<String, String> resolveInputProperties(InputFile inputFile) {
    URI uri = URI.create(inputFile.location());
    if (uri.getScheme() == null) {
      return overlayWithSysProps(Map.of());
    }

    Map<String, String> base =
        inputFile instanceof HadoopInputFile hif
            ? resolvePropertiesFromConf(uri, hif.getConf())
            : Map.of();
    return overlayWithSysProps(base);
  }

  /**
   * Returns {@code base} merged with any {@code -Dvortex.storage.<key>=<value>} system properties.
   * System-property entries override Hadoop-derived ones so users can force a specific credential
   * mode without rebuilding the FileIO.
   */
  private static Map<String, String> overlayWithSysProps(Map<String, String> base) {
    Map<String, String> sysProps = systemPropertyOverrides();
    if (sysProps.isEmpty()) {
      return base;
    }

    Map<String, String> merged = new java.util.HashMap<>(base);
    merged.putAll(sysProps);
    return Map.copyOf(merged);
  }

  private static Map<String, String> systemPropertyOverrides() {
    java.util.Map<String, String> out = new java.util.HashMap<>();
    java.util.Properties props = System.getProperties();
    for (String name : props.stringPropertyNames()) {
      if (name.startsWith(SYSPROP_PREFIX) && name.length() > SYSPROP_PREFIX.length()) {
        out.put(name.substring(SYSPROP_PREFIX.length()), props.getProperty(name));
      }
    }
    return out;
  }

  private static Map<String, String> resolvePropertiesFromConf(URI uri, Configuration conf) {
    Preconditions.checkNotNull(conf, "Hadoop Configuration is required");
    return switch (uri.getScheme()) {
      case "s3a" -> s3PropertiesFromHadoopConf(conf);
      case "wasb", "wasbs", "abfs", "abfss" -> azurePropertiesFromHadoopConf(conf);
      case "file" -> Map.of();
      default -> throw new IllegalArgumentException("Unsupported scheme: " + uri.getScheme());
    };
  }

  private static Map<String, String> s3PropertiesFromHadoopConf(Configuration hadoopConf) {
    VortexS3Properties properties = new VortexS3Properties();
    for (Map.Entry<String, String> entry : hadoopConf) {
      switch (entry.getKey()) {
        case FS_S3A_ACCESS_KEY:
          properties.setAccessKeyId(entry.getValue());
          break;
        case FS_S3A_SECRET_KEY:
          properties.setSecretAccessKey(entry.getValue());
          break;
        case FS_S3A_SESSION_TOKEN:
          properties.setSessionToken(entry.getValue());
          break;
        case FS_S3A_ENDPOINT:
          String qualified = entry.getValue();
          if (!qualified.startsWith("http")) {
            qualified = "https://" + qualified;
          }

          properties.setEndpoint(qualified);
          break;
        case FS_S3A_ENDPOINT_REGION:
          properties.setRegion(entry.getValue());
          break;
        default:
          LOG.trace(
              "Ignoring unknown s3a connector property: {}={}", entry.getKey(), entry.getValue());
          break;
      }
    }

    return properties.asProperties();
  }

  private static Map<String, String> azurePropertiesFromHadoopConf(Configuration hadoopConf) {
    VortexAzureProperties properties = new VortexAzureProperties();
    for (Map.Entry<String, String> entry : hadoopConf) {
      String configKey = entry.getKey();
      if (configKey.startsWith(ACCESS_KEY_PREFIX)) {
        properties.setAccessKey(entry.getValue());
      } else if (configKey.startsWith(FIXED_TOKEN_PREFIX)) {
        properties.setSasKey(entry.getValue());
      } else if (configKey.startsWith(BEARER_TOKEN_PREFIX)) {
        properties.setBearerToken(entry.getValue());
      } else if (configKey.equals(USE_AZURE_CLI_KEY)) {
        properties.setUseAzureCli(Boolean.parseBoolean(entry.getValue()));
      } else {
        LOG.trace("Ignoring unknown azure connector property: {}={}", configKey, entry.getValue());
      }
    }

    return properties.asProperties();
  }
}
