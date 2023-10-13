/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.plugin.aws.s3.connector;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.cdap.cdap.api.annotation.Category;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseEntityPropertyValue;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.aws.s3.common.S3ConnectorConfig;
import io.cdap.plugin.aws.s3.common.S3Constants;
import io.cdap.plugin.aws.s3.common.S3Path;
import io.cdap.plugin.aws.s3.sink.S3BatchSink;
import io.cdap.plugin.aws.s3.source.S3BatchSource;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.format.connector.AbstractFileConnector;
import io.cdap.plugin.format.connector.FileTypeDetector;
import io.cdap.plugin.format.plugin.AbstractFileSourceConfig;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * S3 connector
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(S3Connector.NAME)
@Category("Amazon Web Services")
@Description("Connection to access data in Amazon S3.")
public class S3Connector extends AbstractFileConnector<S3ConnectorConfig> {
  public static final String NAME = "S3";
  private static final String DELIMITER = "/";
  static final String BUCKET_TYPE = "bucket";
  static final String DIRECTORY_TYPE = "directory";
  static final String FILE_TYPE = "file";
  static final String LAST_MODIFIED_KEY = "Last Modified";
  static final String SIZE_KEY = "Size";
  static final String FILE_TYPE_KEY = "File Type";


  private final S3ConnectorConfig config;

  public S3Connector(S3ConnectorConfig config) {
    super(config);
    this.config = config;
    Map<String, String> additionalProperties = Collections.singletonMap(PLUGIN_NAME_PROPERTY_KEY, S3BatchSource.NAME);
    initSampleFields(FILE_TYPE, S3BatchSource.S3BatchConfig.class, additionalProperties);
  }

  @Override
  public void test(ConnectorContext connectorContext) throws ValidationException {
    FailureCollector failureCollector = connectorContext.getFailureCollector();
    config.validate(failureCollector, false);
    // if there is any problem here, that means the credentials are not given so no need to continue
    if (!failureCollector.getValidationFailures().isEmpty()) {
      return;
    }

    AmazonS3 s3 = getS3Client();
    s3.listBuckets();
  }

  @Override
  public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest request) throws IOException {
    String path = request.getPath();
    int limit = request.getLimit() == null || request.getLimit() <= 0 ? Integer.MAX_VALUE : request.getLimit();
    if (isRoot(path)) {
      return browseBuckets(limit);
    }
    return browseObjects(S3Path.from(request.getPath()), limit);
  }

  @Override
  protected String getFullPath(String path) {
    if (isRoot(path)) {
      return S3Path.SCHEME;
    }
    return S3Path.from(path).getFullPath();
  }

  @Override
  protected Map<String, String> getFileSystemProperties(String path) {
    Map<String, String> properties = new HashMap<>();
    if (!config.isAccessCredentials()) {
      return properties;
    }

    properties.put(S3Constants.S3A_ACCESS_KEY, config.getAccessID());
    properties.put(S3Constants.S3A_SECRET_KEY, config.getAccessKey());
    return properties;
  }

  @Override
  protected void setConnectorSpec(ConnectorSpecRequest request, ConnectorSpec.Builder builder) {
    super.setConnectorSpec(request, builder);
    Map<String, String> sourceProperties = new HashMap<>(getAdditionalSpecProperties(request));
    Map<String, String> sinkProperties = new HashMap<>();
    String path = request.getPath();
    String fullPath = getFullPath(path);
    sourceProperties.put(ConfigUtil.NAME_USE_CONNECTION, "true");
    sinkProperties.put(ConfigUtil.NAME_USE_CONNECTION, "true");
    sourceProperties.put(ConfigUtil.NAME_CONNECTION, request.getConnectionWithMacro());
    sinkProperties.put(ConfigUtil.NAME_CONNECTION, request.getConnectionWithMacro());
    sourceProperties.put(S3BatchSource.S3BatchConfig.NAME_PATH, fullPath);
    sinkProperties.put(S3BatchSource.S3BatchConfig.NAME_PATH, fullPath);

    // Only detect format if it has not been set by sample properties
    if (!sourceProperties.containsKey(S3BatchSource.S3BatchConfig.NAME_FORMAT)) {
      sourceProperties.put(AbstractFileSourceConfig.NAME_FORMAT, FileTypeDetector.detectFileFormat(
        FileTypeDetector.detectFileType(path)).name().toLowerCase());
    }

    if (!isRoot(path)) {
      S3Path s3Path = S3Path.from(path);
      String referenceName = ReferenceNames.cleanseReferenceName(s3Path.getBucket() + "." + s3Path.getName());
      sourceProperties.put(Constants.Reference.REFERENCE_NAME, referenceName);
      sinkProperties.put(Constants.Reference.REFERENCE_NAME, referenceName);
    }
    builder.addRelatedPlugin(new PluginSpec(S3BatchSource.NAME, BatchSource.PLUGIN_TYPE, sourceProperties));
    builder.addRelatedPlugin(new PluginSpec(S3BatchSink.NAME, BatchSink.PLUGIN_TYPE, sinkProperties));
  }

  private BrowseDetail browseBuckets(int limit) {
    AmazonS3 s3 = getS3Client();
    List<Bucket> buckets = s3.listBuckets();
    BrowseDetail.Builder builder = BrowseDetail.builder().setTotalCount(buckets.size());
    for (int i = 0; i < Math.min(buckets.size(), limit); i++) {
      String name = buckets.get(i).getName();
      builder.addEntity(BrowseEntity.builder(name, name, BUCKET_TYPE).canBrowse(true).canSample(true).build());
    }
    return builder.build();
  }

  private BrowseDetail browseObjects(S3Path path, int limit) {
    AmazonS3 s3 = getS3Client();
    ListObjectsRequest listObjectsRequest = getListObjectsRequest(path);
    List<BrowseEntity> entities = new ArrayList<>();
    BrowseDetail.Builder builder = BrowseDetail.builder();

    ObjectListing result;
    int count = 0;
    do {
      result = s3.listObjects(listObjectsRequest);
      // common prefixes are directories
      for (String dir : result.getCommonPrefixes()) {
        if (dir.equalsIgnoreCase("/")) {
          continue;
        }
        if (count >= limit) {
          break;
        }
        entities.add(BrowseEntity.builder(new File(dir).getName(), String.format("%s/%s", result.getBucketName(), dir),
                                          DIRECTORY_TYPE).canBrowse(true).canSample(true).build());
        count++;
      }
      for (S3ObjectSummary summary : result.getObjectSummaries()) {
        if (count >= limit) {
          break;
        }

        BrowseEntity build = generateFromSummary(summary);
        entities.add(build);
        count++;
      }
      listObjectsRequest.setMarker(result.getMarker());
    } while (count < limit && result.isTruncated());

    // if the result is empty, this path may already be a file so just try to list it without "/" in prefix
    if (count == 0 && entities.isEmpty()) {
      ListObjectsRequest fileRequest = new ListObjectsRequest();
      fileRequest.setBucketName(path.getBucket());
      fileRequest.setPrefix(path.getName());
      ObjectListing listing = s3.listObjects(fileRequest);
      List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
      if (objectSummaries.isEmpty()) {
        return builder.build();
      }
      return builder.setTotalCount(1).addEntity(generateFromSummary(objectSummaries.get(0)))
        .setSampleProperties(getSampleProperties()).build();
    }
    return builder.setTotalCount(count).setEntities(entities)
      .setSampleProperties(getSampleProperties()).build();
  }

  private BrowseEntity generateFromSummary(S3ObjectSummary summary) {
    String name = summary.getKey();
    // on aws the file name can be empty, it this way the key here will ends with "/"
    BrowseEntity.Builder entity = BrowseEntity.builder(name.endsWith(DELIMITER) ? "" : new File(name).getName(),
                                                       String.format("%s/%s", summary.getBucketName(), name),
                                                       FILE_TYPE);
    Map<String, BrowseEntityPropertyValue> properties = new HashMap<>();
    properties.put(SIZE_KEY, BrowseEntityPropertyValue.builder(
      String.valueOf(summary.getSize()), BrowseEntityPropertyValue.PropertyType.SIZE_BYTES).build());
    properties.put(LAST_MODIFIED_KEY, BrowseEntityPropertyValue.builder(
      String.valueOf(summary.getLastModified()), BrowseEntityPropertyValue.PropertyType.TIMESTAMP_MILLIS).build());
    String fileType = FileTypeDetector.detectFileType(name);
    properties.put(FILE_TYPE_KEY, BrowseEntityPropertyValue.builder(
      fileType, BrowseEntityPropertyValue.PropertyType.STRING).build());
    entity.canSample(FileTypeDetector.isSampleable(fileType));
    entity.setProperties(properties);
    addBrowseSampleDefaultValues(entity, name);
    return entity.build();
  }

  /**
   * Return the list request, the request is like following:
   * 1. Bucket has to be set,
   * 2. The prefix is set if the name after bucket is not empty.
   * 3. "/" has to be provided as delimiter and if prefix is not empty, "/" has to be appended to it, so it is able to
   * group the objects with common prefix together. For example, for following objects:
   * bucket/test/text.txt
   * bucket/test/dir/folder1
   * bucket/test/dir/folder2
   * bucket/test/dir2/folder1
   *
   * if browsing bucket/test, prefix has to be set as "test/" and delimiter has to be "/", this will make sure
   * "dir" is returned as a common key. S3 has no method to list just the current dirctory.
   */
  private ListObjectsRequest getListObjectsRequest(S3Path path) {
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
    listObjectsRequest.setBucketName(path.getBucket());
    String name = path.getName();
    String prefix = name.isEmpty() ? null : name.endsWith(DELIMITER) ? name : name + DELIMITER;
    if (prefix != null) {
      listObjectsRequest.setPrefix(prefix);
    }
    listObjectsRequest.setDelimiter(DELIMITER);
    return listObjectsRequest;
  }

  private AmazonS3 getS3Client() {
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
    if (config.getRegion() != null) {
      builder.setRegion(config.getRegion());
    }

    if (!config.isAccessCredentials()) {
      // use IAM provider to access, this can only work on AWS environment
      return builder.withCredentials(InstanceProfileCredentialsProvider.getInstance()).build();
    }

    if (config.getAccessID() == null || config.getAccessKey() == null) {
      throw new IllegalArgumentException("Access ID and Access Key are not provided");
    }

    AWSCredentials creds;
    if (config.getSessionToken() == null) {
      creds = new BasicAWSCredentials(config.getAccessID(), config.getAccessKey());
    } else {
      creds = new BasicSessionCredentials(config.getAccessID(), config.getAccessKey(), config.getSessionToken());
    }
    return builder.withCredentials(new AWSStaticCredentialsProvider(creds)).build();
  }

  private boolean isRoot(String path) {
    return path.isEmpty() || path.equals(S3Path.ROOT_DIR);
  }
}
