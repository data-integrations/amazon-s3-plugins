/*
 * Copyright © 2015-2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.aws.s3.sink;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.StageContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.aws.s3.common.S3ConnectorConfig;
import io.cdap.plugin.aws.s3.common.S3Constants;
import io.cdap.plugin.aws.s3.common.S3Path;
import io.cdap.plugin.aws.s3.connector.S3Connector;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.format.plugin.AbstractFileSink;
import io.cdap.plugin.format.plugin.AbstractFileSinkConfig;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link S3BatchSink} that stores the data of the latest run of an adapter in S3.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(S3BatchSink.NAME)
@Description("Batch sink to use Amazon S3 as a sink.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = S3Connector.NAME)})
public class S3BatchSink extends AbstractFileSink<S3BatchSink.S3BatchSinkConfig> {
  public static final String NAME = "S3";
  private static final String ENCRYPTION_VALUE = "AES256";
  private static final String ACCESS_CREDENTIALS = "Access Credentials";

  private final S3BatchSinkConfig config;
  private Asset asset;

  public S3BatchSink(S3BatchSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    // create asset for lineage
    String referenceName = Strings.isNullOrEmpty(config.getReferenceName())
      ? ReferenceNames.normalizeFqn(config.getPath())
      : config.getReferenceName();
    asset = Asset.builder(referenceName)
      .setFqn(config.getPath().replace(S3Path.SCHEME, "s3://")).build();

    // super is called down here to avoid instantiating the lineage recorder with a null asset
    super.prepareRun(context);
  }

  @Override
  protected LineageRecorder getLineageRecorder(BatchSinkContext context) {
    return new LineageRecorder(context, asset);
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSinkContext context) {
    // when context is null, it is configure time, by that time, it will always use s3n
    boolean useS3n = context == null ? true : Boolean.valueOf(context.getArguments().get(S3Constants.USE_S3N));
    Map<String, String> properties = new HashMap<>(config.getFilesystemProperties());

    if (config.connection.isAccessCredentials()) {
      if (config.path.startsWith(S3Path.SCHEME) || (config.path.startsWith(S3Path.OLD_SCHEME) && !useS3n)) {
        properties.put(S3Constants.S3A_ACCESS_KEY, config.connection.getAccessID());
        properties.put(S3Constants.S3A_SECRET_KEY, config.connection.getAccessKey());
        if (config.connection.getSessionToken() != null) {
          properties.put(S3Constants.S3A_CREDENTIAL_PROVIDERS, S3Constants.S3A_TEMP_CREDENTIAL_PROVIDERS);
        } else {
          properties.put(S3Constants.S3A_CREDENTIAL_PROVIDERS, S3Constants.S3A_SIMPLE_CREDENTIAL_PROVIDERS);
        }
      } else if (config.path.startsWith("s3n://")) {
        properties.put(S3Constants.S3N_ACCESS_KEY, config.connection.getAccessID());
        properties.put(S3Constants.S3N_SECRET_KEY, config.connection.getAccessKey());
      }
    }

    if (config.shouldEnableEncryption()) {
      if (config.path.startsWith("s3a://")) {
        properties.put(S3Constants.S3A_ENCRYPTION, ENCRYPTION_VALUE);
      } else if (config.path.startsWith("s3n://")) {
        properties.put(S3Constants.S3N_ENCRYPTION, ENCRYPTION_VALUE);
      }
    }
    return properties;
  }

  @Override
  protected void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields) {
    lineageRecorder.recordWrite("Write", "Wrote to S3.", outputFields);
  }

  @VisibleForTesting
  S3BatchSinkConfig getConfig() {
    return config;
  }

  /**
   * S3 Sink configuration.
   */
  @SuppressWarnings("unused")
  public static class S3BatchSinkConfig extends AbstractFileSinkConfig {
    public static final String NAME_USE_CONNECTION = "useConnection";
    public static final String NAME_CONNECTION = "connection";
    private static final String NAME_PATH = "path";
    private static final String NAME_AUTH_METHOD = "authenticationMethod";
    private static final String NAME_FILE_SYSTEM_PROPERTIES = "fileSystemProperties";

    private static final Gson GSON = new Gson();
    private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

    @Macro
    @Description("The S3 path where the data is stored. Example: 's3a://logs' for " +
      "S3AFileSystem or 's3n://logs' for S3NativeFileSystem. Path that starts with s3n:// will by default get " +
      "converted to s3a://.")
    private String path;

    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(ConfigUtil.NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The connection to use.")
    private S3ConnectorConfig connection;

    @Macro
    @Nullable
    @Description("Server side encryption. Defaults to True. " +
      "Sole supported algorithm is AES256.")
    private Boolean enableEncryption;

    @Macro
    @Nullable
    @Description("Any additional properties to use when reading from the filesystem. "
      + "This is an advanced feature that requires knowledge of the properties supported by the underlying filesystem.")
    private String fileSystemProperties;

    S3BatchSinkConfig() {
      // Set default value for Nullable properties.
      this.enableEncryption = false;
      this.fileSystemProperties = GSON.toJson(Collections.emptyMap());
    }

    public void validate() {
      // no-op
    }

    @Override
    public void validate(FailureCollector collector) {
      super.validate(collector);
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
      if (!containsMacro(ConfigUtil.NAME_CONNECTION)) {
        if (connection == null) {
          collector.addFailure("Connection credentials is not provided", "Please provide valid credentials");
        } else {
          connection.validate(collector);
        }
      }

      if (!containsMacro(NAME_PATH) && !path.startsWith("s3a://") && !path.startsWith("s3n://")) {
        collector.addFailure("Path must start with s3a:// or s3n://.", null).withConfigProperty(NAME_PATH);
      }

      if (!containsMacro(NAME_PATH) && path.startsWith("s3n://") && connection != null &&
        !Strings.isNullOrEmpty(connection.getSessionToken())) {
        collector.addFailure("Temporary credentials are only supported for s3a:// paths.", null)
          .withConfigProperty(NAME_PATH);
      }

      if (!containsMacro(NAME_FILE_SYSTEM_PROPERTIES)) {
        try {
          getFilesystemProperties();
        } catch (Exception e) {
          collector.addFailure("File system properties must be a valid json.", null)
            .withConfigProperty(NAME_FILE_SYSTEM_PROPERTIES).withStacktrace(e.getStackTrace());
        }
      }
    }

    @Override
    public String getPath() {
      return path.startsWith(S3Path.OLD_SCHEME) ? S3Path.SCHEME + path.substring(S3Path.OLD_SCHEME.length()) : path;
    }

    @Override
    public String getPath(StageContext context) {
      if (!path.startsWith(S3Path.OLD_SCHEME)) {
        return path;
      }
      return Boolean.valueOf(context.getArguments().get(S3Constants.USE_S3N)) ? path :
               S3Path.SCHEME + path.substring(S3Path.OLD_SCHEME.length());
    }

    boolean shouldEnableEncryption() {
      return enableEncryption;
    }

    Map<String, String> getFilesystemProperties() {
      Map<String, String> properties = new HashMap<>();
      if (containsMacro(NAME_FILE_SYSTEM_PROPERTIES)) {
        return properties;
      }
      return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    }
  }
}
