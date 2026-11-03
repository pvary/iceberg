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

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Diagnostics of the remote locations of {@link BenchmarkFiles}, which report why a location does
 * not work before the benchmark runs.
 *
 * <p>For every trial with a remote location, the setup logs at INFO level:
 *
 * <ul>
 *   <li>the location, the directory of the trial, and the {@code FileIO} of the location
 *   <li>the {@code FileIO} properties, with redacted secrets
 *   <li>for Azure, the account, the container, the endpoint, the credential that {@code ADLSFileIO}
 *       uses, and the Azure environment variables, with redacted secrets
 *   <li>the addresses of the host of the endpoint
 *   <li>every step of a check that writes, reads, and deletes a small file in the directory, with
 *       its time; a step that fails stops the trial with the causes of the failure
 * </ul>
 *
 * <p>The Azure SDK logs more with Azure's environment variables: {@code AZURE_LOG_LEVEL=verbose}
 * logs, for example, which credentials {@code DefaultAzureCredential} tries and why they fail, and
 * {@code AZURE_HTTP_LOG_DETAIL_LEVEL=basic} logs every HTTP request and response.
 */
final class BenchmarkFilesDiagnostics {
  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkFilesDiagnostics.class);

  // the level of the loggers of the Azure SDK, with SLF4J's simple logger of the benchmark jar
  private static final String AZURE_LOGGER_LEVEL = "org.slf4j.simpleLogger.log.com.azure";
  private static final Set<String> AZURE_SCHEMES =
      ImmutableSet.of("abfs", "abfss", "wasb", "wasbs");
  private static final String SAS_TOKEN_PREFIX = "adls.sas-token.";
  private static final String CONNECTION_STRING_PREFIX = "adls.connection-string.";
  private static final String SHARED_KEY_ACCOUNT_NAME = "adls.auth.shared-key.account.name";
  private static final String SHARED_KEY_ACCOUNT_KEY = "adls.auth.shared-key.account.key";
  private static final String TOKEN = "adls.token";
  private static final String TOKEN_CREDENTIAL_PROVIDER = "adls.token-credential-provider";
  // the environment variables of the Azure SDK and of its credentials
  private static final List<String> AZURE_ENVIRONMENT =
      ImmutableList.of(
          "AZURE_CLIENT_ID",
          "AZURE_TENANT_ID",
          "AZURE_CLIENT_SECRET",
          "AZURE_CLIENT_CERTIFICATE_PATH",
          "AZURE_USERNAME",
          "AZURE_PASSWORD",
          "AZURE_FEDERATED_TOKEN_FILE",
          "AZURE_AUTHORITY_HOST",
          "IDENTITY_ENDPOINT",
          "IDENTITY_HEADER",
          "MSI_ENDPOINT",
          "MSI_SECRET",
          "IMDS_ENDPOINT",
          "AZURE_LOG_LEVEL",
          "AZURE_HTTP_LOG_DETAIL_LEVEL",
          "HTTPS_PROXY",
          "HTTP_PROXY",
          "NO_PROXY");
  private static final Set<String> SECRET_ENVIRONMENT =
      ImmutableSet.of("AZURE_CLIENT_SECRET", "AZURE_PASSWORD", "IDENTITY_HEADER", "MSI_SECRET");
  private static final Pattern STATUS_CODE = Pattern.compile("Status code (\\d{3})");
  private static final Pattern ERROR_CODE =
      Pattern.compile("<Code>([^<]+)</Code>|\"code\"\\s*:\\s*\"([^\"]+)\"");
  private static final String DATA_ROLE =
      "assign the Storage Blob Data Contributor role to the identity, on the storage account or"
          + " the container (Owner, Contributor, and Reader do not give access to the data, and"
          + " new role assignments take a few minutes to apply)";
  // hints by the error code of Azure
  private static final Map<String, String> ERROR_CODE_HINTS =
      ImmutableMap.<String, String>builder()
          .put(
              "AuthorizationPermissionMismatch",
              "the identity is authenticated, but has no permission for the operation: "
                  + DATA_ROLE)
          .put(
              "AuthorizationFailure",
              "the request is not authorized: the firewall of the storage account may block the"
                  + " network of this node (check Networking of the storage account), the SAS"
                  + " token has no permission for the operation, or "
                  + DATA_ROLE)
          .put(
              "AuthenticationFailed",
              "the credential is not valid for the account: check the shared key or the SAS token"
                  + " (signature, expiry, permissions, and the host in its property name), and"
                  + " the clock of this node")
          .put(
              "KeyBasedAuthenticationNotPermitted",
              "the storage account does not allow shared keys or SAS tokens: use Entra ID, like"
                  + " the managed identity of the node, without credentials in the properties")
          .put(
              "ContainerNotFound",
              "the container of the location does not exist: create it, or fix the container"
                  + " before the @ in the location")
          .put(
              "FilesystemNotFound",
              "the container of the location does not exist: create it, or fix the container"
                  + " before the @ in the location")
          .put(
              "InvalidAuthenticationInfo",
              "the token is not valid for Azure Storage: check the identity and its tenant")
          .buildOrThrow();
  // hints by the HTTP status, without an error code
  private static final Map<String, String> STATUS_CODE_HINTS =
      ImmutableMap.of(
          "401", "the request is not authenticated: check the credential",
          "403",
              "the request is not authorized: the credential is not valid, the identity has no"
                  + " data role ("
                  + DATA_ROLE
                  + "), or the firewall of the storage account blocks this node",
          "404", "the container or the file does not exist");
  // hints by the type or the message of a failure in the chain of causes
  private static final Map<String, String> FAILURE_HINTS =
      ImmutableMap.<String, String>builder()
          .put(
              "java.net.UnknownHostException",
              "the host of the location does not resolve: check the account name in the"
                  + " location, the DNS of this node, and the private endpoint of the account")
          .put(
              "CredentialUnavailableException",
              "no credential of DefaultAzureCredential is available: on an Azure VM, assign a"
                  + " managed identity to the VM, and set AZURE_CLIENT_ID for a user-assigned"
                  + " one; AZURE_LOG_LEVEL=verbose logs why every credential is unavailable")
          .put(
              "ClientAuthenticationException",
              "the identity cannot get a token: check the identity, its tenant, and"
                  + " AZURE_CLIENT_ID; AZURE_LOG_LEVEL=verbose logs the details")
          .put(
              "requires https",
              "Entra ID tokens need an https endpoint: use wasbs:// or abfss://, and an https"
                  + " endpoint in adls.connection-string.<host>")
          .put(
              "javax.net.ssl.SSLHandshakeException",
              "TLS failed: a proxy may intercept TLS; check the proxy and the trust store")
          .put(
              "java.net.ConnectException",
              "cannot connect to the endpoint: check the network, the proxy (HTTPS_PROXY), and"
                  + " the firewall or private endpoint of the storage account")
          .put(
              "TimeoutException",
              "the request timed out: check the network, the proxy (HTTPS_PROXY), and the"
                  + " firewall or private endpoint of the storage account")
          .buildOrThrow();
  private static final int MAX_CAUSES = 10;
  private static final int MAX_MESSAGE_LENGTH = 500;
  private static final String CHECK_FILE = "access-check";
  private static final byte[] CHECK_CONTENT =
      "Iceberg benchmark access check".getBytes(StandardCharsets.UTF_8);

  private BenchmarkFilesDiagnostics() {}

  /**
   * Sets the level of the loggers of the Azure SDK from Azure's {@code AZURE_LOG_LEVEL} environment
   * variable, unless the {@code org.slf4j.simpleLogger.log.com.azure} system property sets it. The
   * SDK reads the variable only without an SLF4J logger, and SLF4J's simple logger reads the level
   * when a logger is created, so this must run before the SDK is used.
   */
  static void configureAzureLogging() {
    String azureLevel = System.getenv("AZURE_LOG_LEVEL");
    if (azureLevel == null
        || azureLevel.isBlank()
        || System.getProperty(AZURE_LOGGER_LEVEL) != null) {
      return;
    }

    String level =
        switch (azureLevel.trim().toLowerCase(Locale.ROOT)) {
          case "verbose", "debug", "1" -> "debug";
          case "info", "information", "informational", "2" -> "info";
          case "warning", "warn", "3" -> "warn";
          case "error", "err", "4" -> "error";
          default -> null;
        };
    if (level != null) {
      System.setProperty(AZURE_LOGGER_LEVEL, level);
      LOG.info("Logging the Azure SDK at {} level, from AZURE_LOG_LEVEL={}", level, azureLevel);
    } else {
      LOG.warn(
          "Ignoring AZURE_LOG_LEVEL={}, expected verbose, info, warning, or error", azureLevel);
    }
  }

  /** Logs how the files of a trial are accessed. */
  static void logSetup(
      String location,
      String directory,
      String propertiesFile,
      Map<String, String> properties,
      FileIO io) {
    LOG.info(
        "Remote files: location={}, directory={}, FileIO={}",
        location,
        directory,
        ioClass(io, directory));
    LOG.info(
        "FileIO properties from {}: {}",
        propertiesFile.isEmpty() ? "<no properties file>" : propertiesFile,
        redact(properties));

    URI uri;
    try {
      uri = URI.create(location);
    } catch (IllegalArgumentException e) {
      LOG.warn("Cannot parse location {}", location, e);
      return;
    }

    String host = uri.getHost();
    if (uri.getScheme() != null
        && AZURE_SCHEMES.contains(uri.getScheme().toLowerCase(Locale.ROOT))) {
      host = logAzure(uri, properties);
    }

    if (host != null) {
      logAddresses(host);
    }
  }

  private static String ioClass(FileIO io, String directory) {
    if (io instanceof ResolvingFileIO resolving) {
      try {
        return resolving.ioClass(directory).getName();
      } catch (RuntimeException e) {
        return "unavailable: " + e.getMessage();
      }
    }

    return io.getClass().getName();
  }

  /**
   * Logs the Azure account, container, endpoint, and credential of a location, like {@code
   * AzureProperties} configures the clients of {@code ADLSFileIO}, and the Azure environment
   * variables.
   *
   * @return the host of the endpoint
   */
  private static String logAzure(URI uri, Map<String, String> properties) {
    String authority = uri.getRawAuthority() != null ? uri.getRawAuthority() : "";
    int at = authority.indexOf('@');
    String container = at >= 0 ? authority.substring(0, at) : "";
    String host = authority.substring(at + 1);
    String account = host.split("\\.", -1)[0];
    String endpointOverride = properties.get(CONNECTION_STRING_PREFIX + host);
    LOG.info(
        "Azure location: account={}, container={}, path={}, endpoint={}",
        account,
        container,
        uri.getRawPath(),
        endpointOverride != null ? describeEndpoint(endpointOverride) : "https://" + host);
    if (container.isEmpty()) {
      LOG.warn(
          "The location has no container, use {}://<container>@{}/<path>", uri.getScheme(), host);
    }

    if (endpointOverride == null && !host.contains(".core.")) {
      LOG.warn(
          "The host {} is not an Azure Storage host, like <account>.blob.core.windows.net", host);
    }

    LOG.info("Azure credential: {}", azureCredential(host, account, properties));
    warnAboutOtherHosts(host, properties);
    LOG.info("Azure environment variables: {}", azureEnvironment());
    return endpointOverride != null ? endpointHost(endpointOverride, host) : host;
  }

  /** Warns about SAS tokens and endpoints for other hosts, which {@code ADLSFileIO} ignores. */
  private static void warnAboutOtherHosts(String host, Map<String, String> properties) {
    for (String key : properties.keySet()) {
      boolean sasToken = key.startsWith(SAS_TOKEN_PREFIX);
      boolean endpoint = key.startsWith(CONNECTION_STRING_PREFIX);
      String expected = (sasToken ? SAS_TOKEN_PREFIX : CONNECTION_STRING_PREFIX) + host;
      if ((sasToken || endpoint) && !key.equals(expected)) {
        LOG.warn(
            "Ignoring FileIO property {}, which is not for the host of the location: {}",
            key,
            expected);
      }
    }
  }

  /** Returns the Azure environment variables that are set, with redacted secrets. */
  private static Object azureEnvironment() {
    List<String> environment = Lists.newArrayList();
    for (String name : AZURE_ENVIRONMENT) {
      String value = System.getenv(name);
      if (value != null) {
        String shown = SECRET_ENVIRONMENT.contains(name) ? "<redacted>" : redactUserInfo(value);
        environment.add(name + "=" + shown);
      }
    }

    return environment.isEmpty() ? "none" : environment;
  }

  private static String endpointHost(String endpoint, String host) {
    try {
      String endpointHost = URI.create(endpoint).getHost();
      return endpointHost != null ? endpointHost : host;
    } catch (IllegalArgumentException e) {
      return host;
    }
  }

  /** Returns the credential that {@code AzureProperties} configures for a host. */
  private static String azureCredential(
      String host, String account, Map<String, String> properties) {
    String sasToken = properties.get(SAS_TOKEN_PREFIX + host);
    if (sasToken != null && !sasToken.isEmpty()) {
      return "SAS token of " + SAS_TOKEN_PREFIX + host;
    }

    String sharedKeyAccount = properties.get(SHARED_KEY_ACCOUNT_NAME);
    if (sharedKeyAccount != null && properties.containsKey(SHARED_KEY_ACCOUNT_KEY)) {
      if (!sharedKeyAccount.equals(account)) {
        LOG.warn(
            "The shared key is for account {}, but the location is in account {}",
            sharedKeyAccount,
            account);
      }

      return "shared key of account " + sharedKeyAccount;
    }

    String token = properties.get(TOKEN);
    if (token != null && !token.isEmpty()) {
      return "static token of " + TOKEN;
    }

    if (properties.containsKey(TOKEN_CREDENTIAL_PROVIDER)) {
      return "token credential provider " + properties.get(TOKEN_CREDENTIAL_PROVIDER);
    }

    return "DefaultAzureCredential, which tries in order: "
        + defaultCredentialHint()
        + "; AZURE_LOG_LEVEL=verbose logs the credentials that it tries";
  }

  /** Describes the credential that {@code DefaultAzureCredential} uses, by the environment. */
  private static String defaultCredentialHint() {
    boolean clientId = System.getenv("AZURE_CLIENT_ID") != null;
    boolean tenantId = System.getenv("AZURE_TENANT_ID") != null;
    boolean servicePrincipal =
        System.getenv("AZURE_CLIENT_SECRET") != null
            || System.getenv("AZURE_CLIENT_CERTIFICATE_PATH") != null;
    String environment =
        clientId && tenantId && servicePrincipal
            ? "the service principal of the AZURE_* variables (used before a managed identity!)"
            : "a service principal of the AZURE_* variables (not configured)";
    String workloadIdentity =
        System.getenv("AZURE_FEDERATED_TOKEN_FILE") != null
            ? "workload identity (configured)"
            : "workload identity (not configured)";
    String managedIdentity =
        clientId
            ? "the user-assigned managed identity of AZURE_CLIENT_ID"
            : "the system-assigned managed identity (set AZURE_CLIENT_ID for a user-assigned one)";
    return String.join(", ", environment, workloadIdentity, managedIdentity, "the Azure CLI login");
  }

  /** Describes an endpoint override, without the secrets of a connection string. */
  private static String describeEndpoint(String endpoint) {
    return endpoint.matches("^https?://[^?;]*$") ? endpoint : "<redacted connection string>";
  }

  private static Map<String, String> redact(Map<String, String> properties) {
    Map<String, String> redacted = Maps.newTreeMap();
    properties.forEach(
        (key, value) ->
            redacted.put(
                key,
                isSecret(key) && !isEndpoint(key, value)
                    ? String.format(Locale.ROOT, "<redacted, %d chars>", value.length())
                    : value));
    return redacted;
  }

  /** Returns whether a property is an endpoint override, without secrets. */
  private static boolean isEndpoint(String key, String value) {
    return key.startsWith(CONNECTION_STRING_PREFIX) && describeEndpoint(value).equals(value);
  }

  private static boolean isSecret(String key) {
    String lower = key.toLowerCase(Locale.ROOT);
    return lower.contains("sas-token")
        || lower.contains("secret")
        || lower.contains("password")
        || lower.contains("connection-string")
        || lower.endsWith(".key")
        || lower.equals(TOKEN);
  }

  private static String redactUserInfo(String value) {
    return value.replaceAll("//[^/@]*@", "//<redacted>@");
  }

  /** Logs the addresses of a host, which show DNS and private endpoint issues. */
  private static void logAddresses(String host) {
    long start = System.nanoTime();
    try {
      InetAddress[] addresses = InetAddress.getAllByName(host);
      LOG.info(
          "Host {} resolves to {} ({} ms)", host, Arrays.toString(addresses), elapsedMillis(start));
    } catch (UnknownHostException e) {
      LOG.warn("Cannot resolve host {} ({} ms)", host, elapsedMillis(start), e);
    }
  }

  /**
   * Writes, reads, and deletes a small file in a directory, and logs every step, so that a location
   * that does not work fails before the benchmark, with the causes of the failure.
   */
  static void checkAccess(FileIO io, String directory) throws IOException {
    String path = directory + "/" + CHECK_FILE;
    step(
        "write",
        path,
        () -> {
          try (PositionOutputStream out = io.newOutputFile(path).createOrOverwrite()) {
            out.write(CHECK_CONTENT);
          }
        });
    step(
        "get the length of",
        path,
        () -> {
          long length = io.newInputFile(path).getLength();
          if (length != CHECK_CONTENT.length) {
            throw new IOException(
                String.format("Invalid length: %s, expected %s", length, CHECK_CONTENT.length));
          }
        });
    step(
        "read",
        path,
        () -> {
          byte[] content;
          try (SeekableInputStream in = io.newInputFile(path, CHECK_CONTENT.length).newStream()) {
            content = in.readNBytes(CHECK_CONTENT.length + 1);
          }

          if (!Arrays.equals(content, CHECK_CONTENT)) {
            throw new IOException("Read different content than written");
          }
        });

    long start = System.nanoTime();
    try {
      io.deleteFile(path);
      if (io.newInputFile(path).exists()) {
        LOG.warn(
            "Access check: cannot delete {} ({} ms), so the benchmark files will stay in {}",
            path,
            elapsedMillis(start),
            directory);
      } else {
        LOG.info("Access check: delete {}: OK ({} ms)", path, elapsedMillis(start));
      }
    } catch (RuntimeException e) {
      LOG.warn(
          "Access check: cannot delete {} ({} ms), so the benchmark files will stay in {}: {}",
          path,
          elapsedMillis(start),
          directory,
          describe(e),
          e);
    }
  }

  private interface Action {
    void run() throws IOException;
  }

  private static void step(String action, String path, Action step) throws IOException {
    long start = System.nanoTime();
    try {
      step.run();
    } catch (IOException | RuntimeException e) {
      String causes = describe(e);
      LOG.error(
          "Access check: cannot {} {} ({} ms): {}", action, path, elapsedMillis(start), causes, e);
      throw new IOException(String.format("Cannot %s %s: %s", action, path, causes), e);
    }

    LOG.info("Access check: {} {}: OK ({} ms)", action, path, elapsedMillis(start));
  }

  /**
   * Describes a failure: its likely cause, from the status and error code of Azure or the type of
   * the failure, and its chain of causes.
   */
  static String describe(Throwable failure) {
    List<String> causes = Lists.newArrayList();
    StringBuilder details = new StringBuilder();
    String previousMessage = null;
    Throwable cause = failure;
    while (cause != null && causes.size() < MAX_CAUSES) {
      String message = cause.getMessage() != null ? cause.getMessage() : "";
      details.append(cause.getClass().getName()).append(": ").append(message).append('\n');
      // wrapping exceptions often repeat the message of their cause
      if (previousMessage == null || !previousMessage.contains(message) || message.isEmpty()) {
        causes.add(describeCause(cause, message));
      }

      previousMessage = message;
      cause = cause.getCause() != cause ? cause.getCause() : null;
    }

    String chain = String.join("; caused by ", causes);
    String likelyCause = likelyCause(details.toString());
    return likelyCause != null ? likelyCause + " Failure: " + chain : chain;
  }

  private static String describeCause(Throwable cause, String message) {
    String shown = message.replaceAll("\\s+", " ").trim();
    if (shown.length() > MAX_MESSAGE_LENGTH) {
      shown = shown.substring(0, MAX_MESSAGE_LENGTH) + "...";
    }

    return cause.getClass().getSimpleName() + (shown.isEmpty() ? "" : ": " + shown);
  }

  /** Returns the likely cause of a failure, with the details of its chain of causes. */
  private static String likelyCause(String details) {
    Matcher status = STATUS_CODE.matcher(details);
    String statusCode = status.find() ? status.group(1) : null;
    String errorCode = errorCode(details);
    String hint = hint(statusCode, errorCode, details);
    if (hint == null && statusCode == null && errorCode == null) {
      return null;
    }

    List<String> codes = Lists.newArrayList();
    if (statusCode != null) {
      codes.add("status " + statusCode);
    }

    if (errorCode != null) {
      codes.add(errorCode);
    }

    String described = codes.isEmpty() ? "" : " (" + String.join(", ", codes) + ")";
    return "Likely cause" + described + ": " + (hint != null ? hint : "see the failure") + ".";
  }

  /** Returns a hint by the error code, then by the failures, then by the status code. */
  private static String hint(String statusCode, String errorCode, String details) {
    String hint = errorCode != null ? ERROR_CODE_HINTS.get(errorCode) : null;
    if (hint == null) {
      hint = failureHint(details);
    }

    return hint == null && statusCode != null ? STATUS_CODE_HINTS.get(statusCode) : hint;
  }

  /** Returns the error code of Azure, from an XML (Blob) or JSON (Data Lake) error response. */
  private static String errorCode(String details) {
    Matcher code = ERROR_CODE.matcher(details);
    if (code.find()) {
      return code.group(1) != null ? code.group(1) : code.group(2);
    }

    return null;
  }

  private static String failureHint(String details) {
    for (Map.Entry<String, String> hint : FAILURE_HINTS.entrySet()) {
      if (details.contains(hint.getKey())) {
        return hint.getValue();
      }
    }

    return null;
  }

  private static long elapsedMillis(long startNanos) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
  }
}
