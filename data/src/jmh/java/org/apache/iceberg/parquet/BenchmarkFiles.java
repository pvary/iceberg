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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The files of a benchmark trial, in a new directory, which is deleted with the files when the
 * trial ends.
 *
 * <p>Without a location, the directory is a new local temporary directory, and the files are read
 * and written with {@link Files#localInput} and {@link Files#localOutput}. With a location, the
 * directory is a new directory under the location, and the files are read and written with
 * Iceberg's {@link ResolvingFileIO}, which picks the {@code FileIO} by the scheme of the location,
 * like Iceberg's REST catalog clients: {@code ADLSFileIO} for Azure Blob Storage and ADLS Gen2
 * ({@code abfs://}, {@code abfss://}, {@code wasb://} and {@code wasbs://} locations, for example
 * {@code abfss://<container>@<account>.dfs.core.windows.net/<path>}), and {@code HadoopFileIO} for
 * Hadoop file systems, like {@code file:/tmp/benchmarks}.
 *
 * <p>The properties of the {@code FileIO}, like credentials, are read from a local Java properties
 * file, so that secrets are not part of the benchmark parameters, which JMH prints and stores with
 * the results. For Azure, the properties are the ones of {@code AzureProperties}, for example:
 *
 * <ul>
 *   <li>a shared key: {@code adls.auth.shared-key.account.name} and {@code
 *       adls.auth.shared-key.account.key}
 *   <li>a SAS token: {@code adls.sas-token.<account>.dfs.core.windows.net}, with the host of the
 *       location
 *   <li>the size of the blocks read and written: {@code adls.read.block-size-bytes} and {@code
 *       adls.write.block-size-bytes}
 * </ul>
 *
 * <p>Without credentials, {@code ADLSFileIO} authenticates with Azure's {@code
 * DefaultAzureCredential}, for example with the {@code AZURE_CLIENT_ID}, {@code AZURE_TENANT_ID}
 * and {@code AZURE_CLIENT_SECRET} environment variables, with the managed identity of the node, or
 * with the login of the Azure CLI.
 *
 * <p>For remote locations, the setup logs how the files are accessed, and checks that a small file
 * can be written, read, and deleted, so that a location that does not work fails before the
 * benchmark, with the causes of the failure (see {@link BenchmarkFilesDiagnostics}).
 */
final class BenchmarkFiles implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkFiles.class);
  // the URL of a container, like https://<account>.blob.core.windows.net/<container>/<path>
  private static final Pattern CONTAINER_URL =
      Pattern.compile("^https?://([^/?#]+)/([^/?#]+)/?([^?#]*)$");

  // null for local files
  private final ResolvingFileIO io;
  private final String directory;
  private final List<String> names = Lists.newArrayList();

  private BenchmarkFiles(ResolvingFileIO io, String directory) {
    this.io = io;
    this.directory = directory;
  }

  /**
   * Creates the files of a trial in a new directory.
   *
   * @param location base location of the directory, or empty for a local temporary directory
   * @param propertiesFile path of a local Java properties file with the properties of the {@code
   *     FileIO} of the location, or empty for none; ignored for local files, so that local and
   *     remote locations can be compared in the same run
   * @param prefix prefix of the name of the directory
   */
  static BenchmarkFiles create(String location, String propertiesFile, String prefix)
      throws IOException {
    if (location.isEmpty()) {
      File directory = java.nio.file.Files.createTempDirectory(prefix).toFile();
      return new BenchmarkFiles(null, directory.getPath());
    }

    checkNotUrl(location);
    // before the Azure SDK creates its loggers
    BenchmarkFilesDiagnostics.configureAzureLogging();
    Map<String, String> properties = loadProperties(propertiesFile);
    ResolvingFileIO io = new ResolvingFileIO();
    // for HadoopFileIO, which ResolvingFileIO uses for the locations of Hadoop file systems
    Configuration conf = new Configuration();
    // the benchmark jar keeps only one of the FileSystem service files of the Hadoop jars, which
    // does not register the local file system
    conf.setIfUnset("fs.file.impl", LocalFileSystem.class.getName());
    io.setConf(conf);
    io.initialize(properties);
    String directory =
        LocationUtil.stripTrailingSlash(location) + "/" + prefix + "-" + UUID.randomUUID();
    BenchmarkFiles files = new BenchmarkFiles(io, directory);
    try {
      BenchmarkFilesDiagnostics.logSetup(location, directory, propertiesFile, properties, io);
      BenchmarkFilesDiagnostics.checkAccess(io, directory);
    } catch (IOException | RuntimeException e) {
      try {
        files.close();
      } catch (IOException | RuntimeException closeFailure) {
        e.addSuppressed(closeFailure);
      }

      throw e;
    }

    return files;
  }

  /**
   * Rejects HTTP URLs, like the ones Azure shows for containers, which are not FileIO locations,
   * and suggests the location of the container.
   */
  private static void checkNotUrl(String location) {
    Matcher url = CONTAINER_URL.matcher(location);
    if (url.matches()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid location: %s, use a FileIO location, like wasbs://%s@%s/%s for Azure Blob "
                  + "Storage, or abfss://<container>@<account>.dfs.core.windows.net/<path> for ADLS "
                  + "Gen2",
              location, url.group(2), url.group(1), url.group(3)));
    } else if (location.startsWith("http://") || location.startsWith("https://")) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid location: %s, use a FileIO location, like "
                  + "wasbs://<container>@<account>.blob.core.windows.net/<path> for Azure Blob Storage",
              location));
    }
  }

  private static Map<String, String> loadProperties(String propertiesFile) throws IOException {
    if (propertiesFile.isEmpty()) {
      return ImmutableMap.of();
    }

    Properties properties = new Properties();
    try (InputStream in = java.nio.file.Files.newInputStream(Paths.get(propertiesFile))) {
      properties.load(in);
    }

    return Maps.fromProperties(properties);
  }

  /** Returns the file with the given name in the directory, to write it. */
  OutputFile newOutputFile(String name) {
    if (!names.contains(name)) {
      names.add(name);
    }

    String path = path(name);
    return io != null ? io.newOutputFile(path) : Files.localOutput(path);
  }

  /** Returns the file with the given name in the directory, to read it. */
  InputFile newInputFile(String name) {
    String path = path(name);
    return io != null ? io.newInputFile(path) : Files.localInput(path);
  }

  /**
   * Returns the file with the given name and length in the directory, to read it without looking up
   * its length, like Iceberg reads data files with the lengths from the manifests.
   */
  InputFile newInputFile(String name, long length) {
    String path = path(name);
    return io != null ? io.newInputFile(path, length) : Files.localInput(path);
  }

  private String path(String name) {
    return io != null ? directory + "/" + name : new File(directory, name).getPath();
  }

  @Override
  public String toString() {
    return io != null
        ? String.format("%s (%s)", directory, io.ioClass(directory).getSimpleName())
        : String.format("%s (local)", directory);
  }

  /** Deletes the files and the directory, and closes the {@code FileIO}. */
  @Override
  public void close() throws IOException {
    if (io == null) {
      for (String name : names) {
        java.nio.file.Files.deleteIfExists(Paths.get(path(name)));
      }

      java.nio.file.Files.deleteIfExists(Paths.get(directory));
      return;
    }

    try {
      for (String name : names) {
        String path = path(name);
        if (io.newInputFile(path).exists()) {
          io.deleteFile(path);
        }
      }

      try {
        // deletes the directory, for file systems that have directories, like ADLS Gen2
        io.deletePrefix(directory);
        LOG.info("Deleted the benchmark files in {}", directory);
      } catch (RuntimeException e) {
        LOG.warn(
            "Failed to delete directory {}: {}",
            directory,
            BenchmarkFilesDiagnostics.describe(e),
            e);
      }
    } finally {
      io.close();
    }
  }
}
