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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseEntityPropertyValue;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;
import io.cdap.plugin.aws.s3.common.S3ConnectorConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * S3 connector
 * access.key - access key to create s3 client
 * secret.key - secret key to create s3 client
 * s3.region - region the s3 client to operate on
 * s3.bucket - bucket to operate on, it must not already exist
 */
public class S3ConnectorTest {
  private static String accessKey;
  private static String secretKey;
  private static String region;
  private static String bucket;
  private static AmazonS3 s3Client;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    // Certain properties need to be configured otherwise the whole tests will be skipped.

    String messageTemplate = "%s is not configured, please refer to javadoc of this class for details.";

    accessKey = System.getProperty("access.key");
    Assume.assumeFalse(String.format(messageTemplate, "project id"), accessKey == null);

    secretKey = System.getProperty("secret.key");
    Assume.assumeFalse(String.format(messageTemplate, "bucket"), secretKey == null);

    region = System.getProperty("s3.region");
    Assume.assumeFalse(String.format(messageTemplate, "service account key file"), region == null);

    bucket = System.getProperty("s3.bucket");
    Assume.assumeFalse(String.format(messageTemplate, "bucket"), bucket == null);


    BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
    builder.setRegion(region);
    s3Client = builder.withCredentials(new AWSStaticCredentialsProvider(creds)).build();
    Assume.assumeFalse("The bucket already exists.", s3Client.doesBucketExist(bucket));
  }

  @Before
  public void setUp() throws Exception {
    s3Client.createBucket(bucket);
  }

  @After
  public void tearDown() throws Exception {
    ObjectListing listing = s3Client.listObjects(bucket);
    for (S3ObjectSummary summary : listing.getObjectSummaries()) {
      s3Client.deleteObject(bucket, summary.getKey());
    }
    s3Client.deleteBucket(bucket);
  }

  @Test
  public void testS3Connector() throws Exception {
    // create data
    List<BrowseEntity> entities = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      // file0.txt
      String fileName = "file" + i + ".txt";
      s3Client.putObject(bucket, fileName, "Hello, World");

      // test0/text0.txt
      String folderName = "test" + i;
      fileName = folderName + "/text" + i + ".txt";
      s3Client.putObject(bucket, fileName, "Hello, World");

      // add folder
      entities.add(BrowseEntity.builder(folderName, bucket + "/" + folderName + "/", S3Connector.DIRECTORY_TYPE)
                     .canSample(true).canBrowse(true).build());
    }

    // have to list files to add it
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
    listObjectsRequest.setBucketName(bucket);
    listObjectsRequest.setDelimiter("/");
    ObjectListing listing = s3Client.listObjects(listObjectsRequest);
    listing.getObjectSummaries().forEach(
      summary -> entities.add(
        BrowseEntity.builder(summary.getKey(), bucket + "/" + summary.getKey(), S3Connector.FILE_TYPE).canSample(true)
          .setProperties(getFileProperties(summary)).build()));

    S3Connector connector = new S3Connector(
        new S3ConnectorConfig(accessKey, secretKey, null, "Access Credentials", region));
    MockConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    connector.test(context);
    Assert.assertTrue(context.getFailureCollector().getValidationFailures().isEmpty());

    // browse bucket, here just check if it contains bucket since we don't know if there are other buckets
    BrowseDetail detail = connector.browse(context, BrowseRequest.builder("/").build());

    Assert.assertTrue(detail.getTotalCount() > 0);

    // browse bucket
    for (BrowseEntity entity : detail.getEntities()) {
      Assert.assertEquals(S3Connector.BUCKET_TYPE, entity.getType());
      Assert.assertTrue(entity.canBrowse());
      Assert.assertTrue(entity.canSample());
    }

    // browse blob
    detail = connector.browse(context, BrowseRequest.builder("/" + bucket).build());
    BrowseDetail expected = BrowseDetail.builder().setTotalCount(10).setEntities(entities).build();
    Assert.assertEquals(expected, detail);

    // browse limited
    detail = connector.browse(context, BrowseRequest.builder("/" + bucket).setLimit(5).build());
    expected = BrowseDetail.builder().setTotalCount(5).setEntities(entities.subList(0, 5)).build();
    Assert.assertEquals(expected, detail);

    // browse one single file
    detail = connector.browse(context, BrowseRequest.builder("/" + bucket + "/" + "file0.txt").build());
    // the first file is at index 5
    expected = BrowseDetail.builder().setTotalCount(1).setEntities(entities.subList(5, 6)).build();
    Assert.assertEquals(expected, detail);
  }

  private Map<String, BrowseEntityPropertyValue> getFileProperties(S3ObjectSummary summary) {
    return ImmutableMap.of(
      S3Connector.FILE_TYPE_KEY, BrowseEntityPropertyValue.builder(
        "text/plain", BrowseEntityPropertyValue.PropertyType.STRING).build(),
      S3Connector.LAST_MODIFIED_KEY, BrowseEntityPropertyValue.builder(
        String.valueOf(summary.getLastModified()), BrowseEntityPropertyValue.PropertyType.TIMESTAMP_MILLIS).build(),
      S3Connector.SIZE_KEY, BrowseEntityPropertyValue.builder(
        String.valueOf(summary.getSize()), BrowseEntityPropertyValue.PropertyType.SIZE_BYTES).build());
  }
}
