/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.plugin.aws.s3.source;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.StageContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.aws.s3.common.S3ConnectorConfig;
import io.cdap.plugin.aws.s3.common.S3Constants;
import io.cdap.plugin.aws.s3.common.S3Path;
import io.cdap.plugin.aws.s3.connector.S3Connector;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import io.cdap.plugin.format.plugin.AbstractFileSource;
import io.cdap.plugin.format.plugin.AbstractFileSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSource} that reads from Amazon S3.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(S3BatchSource.NAME)
@Description("Batch source to use Amazon S3 as a source.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = S3Connector.NAME)})
public class S3BatchSource extends AbstractFileSource<S3BatchSource.S3BatchConfig> {
  public static final String NAME = "S3";
  private Asset asset;

  @SuppressWarnings("unused")
  private final S3BatchConfig config;

  public S3BatchSource(S3BatchConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
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
  protected LineageRecorder getLineageRecorder(BatchSourceContext context) {
    return new LineageRecorder(context, asset);
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSourceContext context) {
    // when context is null, it is configure time, by that time, it will always use s3n
    boolean useS3n = context == null ? true : Boolean.valueOf(context.getArguments().get(S3Constants.USE_S3N));
    Map<String, String> properties = new HashMap<>(config.getFilesystemProperties());
    if (config.connection.isAccessCredentials()) {
      // use s3a credential if path starts with s3n and there is no runtime argument to keep using s3n path
      if (config.path.startsWith(S3Path.SCHEME) || (config.path.startsWith(S3Path.OLD_SCHEME) && !useS3n)) {
        properties.put(S3Constants.S3A_ACCESS_KEY, config.connection.getAccessID());
        properties.put(S3Constants.S3A_SECRET_KEY, config.connection.getAccessKey());
        if (config.connection.getSessionToken() != null) {
          properties.put(S3Constants.S3A_CREDENTIAL_PROVIDERS, S3Constants.S3A_TEMP_CREDENTIAL_PROVIDERS);
          properties.put(S3Constants.S3A_SESSION_TOKEN, config.connection.getSessionToken());
        } else {
          properties.put(S3Constants.S3A_CREDENTIAL_PROVIDERS, S3Constants.S3A_SIMPLE_CREDENTIAL_PROVIDERS);
        }
      } else if (config.path.startsWith("s3n://")) {
        properties.put(S3Constants.S3N_ACCESS_KEY, config.connection.getAccessID());
        properties.put(S3Constants.S3N_SECRET_KEY, config.connection.getAccessKey());
      }
    }
    if (config.shouldCopyHeader()) {
      properties.put(PathTrackingInputFormat.COPY_HEADER, "true");
    }
    if (config.getFileEncoding() != null && !config.getFileEncoding().equals(config.getDefaultFileEncoding())) {
      properties.put(PathTrackingInputFormat.SOURCE_FILE_ENCODING, config.getFileEncoding());
    }
    return properties;
  }

  @Override
  protected void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields) {
    lineageRecorder.recordRead("Read", "Read from S3.", outputFields);
  }

  @Override
  protected boolean shouldGetSchema() {
    return !config.containsMacro(S3BatchConfig.NAME_PATH) && !config.containsMacro(S3BatchConfig.NAME_FORMAT) &&
      !config.containsMacro(S3BatchConfig.NAME_DELIMITER) && !config.containsMacro(S3ConnectorConfig.NAME_ACCESS_ID)
      && !config.containsMacro(S3BatchConfig.NAME_FILE_SYSTEM_PROPERTIES) &&
      !config.containsMacro(S3ConnectorConfig.NAME_ACCESS_KEY)
             && !config.containsMacro(S3ConnectorConfig.NAME_ACCESS_KEY);
  }

  /**
   * Config class that contains properties needed for the S3 source.
   */
  @SuppressWarnings("unused")
  public static class S3BatchConfig extends AbstractFileSourceConfig {
    private static final Logger LOG = LoggerFactory.getLogger(S3BatchConfig.class);
    public static final String NAME_PATH = "path";
    private static final String NAME_FILE_SYSTEM_PROPERTIES = "fileSystemProperties";
    private static final String NAME_DELIMITER = "delimiter";
    private static final String NAME_SHEET = "sheet";
    private static final String NAME_SHEET_VALUE = "sheetValue";
    private static final String NAME_TERMINATE_IF_EMPTY_ROW = "terminateIfEmptyRow";

    private static final Gson GSON = new Gson();
    private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

    @Macro
    @Description("Path to file(s) to be read. If a directory is specified, terminate the path name with a '/'. " +
      "The path must start with s3a:// or s3n://. Path that starts with s3n:// will by default get " +
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
    @Description("Any additional properties to use when reading from the filesystem. " +
      "This is an advanced feature that requires knowledge of the properties supported by the underlying filesystem.")
    private String fileSystemProperties;
    
    @Nullable
    @Description("Whether to verify the access credentials. When false, validation will succeed even if the " +
      "credentials are incorrect. When true, the accuracy of the credentials will be evaluated and validation will " +
      "fail, if credentials are incorrect. The default value is false.")
    private Boolean verifyCredentials;

    @Macro
    @Nullable
    @Description("The maximum number of rows that will get investigated for automatic data type detection.")
    private Long sampleSize;

    @Macro
    @Nullable
    @Description("A list of columns with the corresponding data types for whom the automatic data type detection gets" +
            " skipped.")
    private String override;

    @Name(NAME_SHEET)
    @Macro
    @Nullable
    @Description("Select the sheet by name or number. Default is 'Sheet Number'.")
    private String sheet;

    @Name(NAME_SHEET_VALUE)
    @Macro
    @Nullable
    @Description("The name/number of the sheet to read from. If not specified, the first sheet will be read." +
            "Sheet Number are 0 based, ie first sheet is 0.")
    private String sheetValue;

    @Name(NAME_TERMINATE_IF_EMPTY_ROW)
    @Macro
    @Nullable
    @Description("Specify whether to stop reading after encountering the first empty row. Defaults to false.")
    private String terminateIfEmptyRow;

    private S3BatchConfig(String path, @Nullable S3ConnectorConfig connection, String fileSystemProperties,
                          Boolean verifyCredentials) {
      super();
      this.path = path;
      this.connection = connection;
      this.fileSystemProperties = fileSystemProperties;
      this.verifyCredentials = verifyCredentials;
    }

    public S3BatchConfig() {
      fileSystemProperties = GSON.toJson(Collections.emptyMap());
    }

    @Override
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
          connection.validate(collector, shouldVerifyCredentials());
        }
      }
      if (!containsMacro("path") && (!path.startsWith("s3a://") && !path.startsWith("s3n://"))) {
        collector.addFailure("Path must start with s3a:// or s3n://.", null).withConfigProperty(NAME_PATH);
      }
      if (!containsMacro("path") && path.startsWith("s3n://") && !Strings.isNullOrEmpty(connection.getSessionToken())) {
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

    public S3ConnectorConfig getConnection() {
      return connection;
    }

    Map<String, String> getFilesystemProperties() {
      Map<String, String> properties = new HashMap<>();
      if (containsMacro("fileSystemProperties")) {
        return properties;
      }
      return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    }

    public boolean shouldVerifyCredentials() {
      return verifyCredentials == null ? false : verifyCredentials;
    }

    /**
     * S3 Batch Source configuration builder.
     */
    public static class Builder {
      private String path;
      private S3ConnectorConfig connection;
      private String fileSystemProperties;
      private Boolean verifyCredentials;

      public Builder setPath(String path) {
        this.path = path;
        return this;
      }

      public Builder setConnection(@Nullable S3ConnectorConfig connection) {
        this.connection = connection;
        return this;
      }

      public Builder setFileSystemProperties(@Nullable String fileSystemProperties) {
        this.fileSystemProperties = fileSystemProperties;
        return this;
      }
      
      public Builder setVerifyCredentials(Boolean verifyCredentials) {
        this.verifyCredentials = verifyCredentials;
        return this;
      }
      
      public S3BatchSource.S3BatchConfig build() {
        return new S3BatchSource.S3BatchConfig(path, connection, fileSystemProperties, verifyCredentials);
      }

    }
  }
}
