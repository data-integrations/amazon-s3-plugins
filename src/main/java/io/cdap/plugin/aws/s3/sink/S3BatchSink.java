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
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
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
@Name("S3")
@Description("Batch sink to use Amazon S3 as a sink.")
public class S3BatchSink extends AbstractFileSink<S3BatchSink.S3BatchSinkConfig> {
  private static final String ENCRYPTION_VALUE = "AES256";
  private static final String S3A_ACCESS_KEY = "fs.s3a.access.key";
  private static final String S3A_SECRET_KEY = "fs.s3a.secret.key";
  private static final String S3A_ENCRYPTION = "fs.s3a.server-side-encryption-algorithm";

  private static final String S3N_ACCESS_KEY = "fs.s3n.awsAccessKeyId";
  private static final String S3N_SECRET_KEY = "fs.s3n.awsSecretAccessKey";
  private static final String S3N_ENCRYPTION = "fs.s3n.server-side-encryption-algorithm";
  private static final String ACCESS_CREDENTIALS = "Access Credentials";

  private final S3BatchSinkConfig config;

  public S3BatchSink(S3BatchSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSinkContext context) {
    Map<String, String> properties = new HashMap<>(config.getFilesystemProperties());

    if (ACCESS_CREDENTIALS.equalsIgnoreCase(config.authenticationMethod)) {
      if (config.path.startsWith("s3a://")) {
        properties.put(S3A_ACCESS_KEY, config.accessID);
        properties.put(S3A_SECRET_KEY, config.accessKey);
      } else if (config.path.startsWith("s3n://")) {
        properties.put(S3N_ACCESS_KEY, config.accessID);
        properties.put(S3N_SECRET_KEY, config.accessKey);
      }
    }

    if (config.shouldEnableEncryption()) {
      if (config.path.startsWith("s3a://")) {
        properties.put(S3A_ENCRYPTION, ENCRYPTION_VALUE);
      } else if (config.path.startsWith("s3n://")) {
        properties.put(S3N_ENCRYPTION, ENCRYPTION_VALUE);
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
    private static final String NAME_ACCESS_ID = "accessID";
    private static final String NAME_ACCESS_KEY = "accessKey";
    private static final String NAME_PATH = "path";
    private static final String NAME_AUTH_METHOD = "authenticationMethod";
    private static final String NAME_FILE_SYSTEM_PROPERTIES = "fileSystemProperties";

    private static final Gson GSON = new Gson();
    private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

    @Macro
    @Description("The S3 path where the data is stored. Example: 's3a://logs' for " +
      "S3AFileSystem or 's3n://logs' for S3NativeFileSystem.")
    private String path;

    @Macro
    @Nullable
    @Description("Access ID of the Amazon S3 instance to connect to.")
    private String accessID;

    @Macro
    @Nullable
    @Description("Access Key of the Amazon S3 instance to connect to.")
    private String accessKey;

    @Macro
    @Nullable
    @Description("Authentication method to access S3. " +
      "Defaults to Access Credentials. URI scheme should be s3a:// or s3n://.")
    private String authenticationMethod;

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
      this.authenticationMethod = ACCESS_CREDENTIALS;
      this.fileSystemProperties = GSON.toJson(Collections.emptyMap());
    }

    public void validate() {
      // no-op
    }

    @Override
    public void validate(FailureCollector collector) {
      super.validate(collector);
      if (ACCESS_CREDENTIALS.equalsIgnoreCase(authenticationMethod)) {
        if (!containsMacro(NAME_ACCESS_ID) && (accessID == null || accessID.isEmpty())) {
          collector.addFailure("The Access ID must be specified if authentication method is Access Credentials.", null)
            .withConfigProperty(NAME_ACCESS_ID).withConfigProperty(NAME_AUTH_METHOD);
        }
        if (!containsMacro(NAME_ACCESS_KEY) && (accessKey == null || accessKey.isEmpty())) {
          collector.addFailure("The Access Key must be specified if authentication method is Access Credentials.", null)
            .withConfigProperty(NAME_ACCESS_KEY).withConfigProperty(NAME_AUTH_METHOD);
        }
      }

      if (!containsMacro(NAME_PATH) && !path.startsWith("s3a://") && !path.startsWith("s3n://")) {
        collector.addFailure("Path must start with s3a:// or s3n://.", null).withConfigProperty(NAME_PATH);
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
      return path;
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
