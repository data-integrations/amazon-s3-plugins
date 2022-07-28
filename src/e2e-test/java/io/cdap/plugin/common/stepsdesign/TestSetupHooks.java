/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.common.stepsdesign;

import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.S3Client;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang.StringUtils;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.UUID;

/**
 * S3 test hooks.
 */
public class TestSetupHooks {
  public static String s3SourceBucketName = StringUtils.EMPTY;
  public static String s3TargetBucketName = StringUtils.EMPTY;

  @Before(order = 0)
  public static void overrideAWSCredentialsIfProvided() {
    String accessKey = System.getenv("accessKey");
    if (accessKey != null && !accessKey.isEmpty()) {
      PluginPropertyUtils.addPluginProp("accessKey", accessKey);
    }
    String accessID = System.getenv("accessID");
    if (accessID != null && !accessID.isEmpty()) {
      PluginPropertyUtils.addPluginProp("accessID", accessID);
    }
  }

  @Before(order = 1, value = "@S3_SOURCE_TEST")
  public static void createS3BucketWithCSVFile() throws URISyntaxException, IOException {
    s3SourceBucketName = S3Client.createBucket("e2e-test-" + UUID.randomUUID());
    String filePath = PluginPropertyUtils.pluginProp("testCSV");
    S3Client.uploadObject(s3SourceBucketName, filePath);
    BeforeActions.scenario.write(
      "Created S3 Source Bucket " + s3SourceBucketName + " containing " + filePath + " file"
    );
    PluginPropertyUtils.addPluginProp(
      "sourcePath", "s3a://" + s3SourceBucketName + "/" + PluginPropertyUtils.pluginProp("sourcePath")
    );
  }

  @Before(order = 1, value = "@S3_SINK_TEST")
  public static void createTempTargetS3Bucket() throws IOException {
    s3TargetBucketName = S3Client.createBucket("e2e-test-" + UUID.randomUUID());
    BeforeActions.scenario.write("Created S3 Target Bucket " + s3TargetBucketName);
    PluginPropertyUtils.addPluginProp("sinkPath", "s3a://" + s3TargetBucketName);
  }


  @After(order = 1, value = "@S3_SOURCE_TEST")
  public static void deleteS3BucketWithCSVFile() throws IOException {
    S3Client.deleteBucket(s3SourceBucketName);
    BeforeActions.scenario.write("Deleted S3 Source Bucket " + s3SourceBucketName);
    s3SourceBucketName = StringUtils.EMPTY;

  }

  @After(order = 1, value = "@S3_SINK_TEST")
  public static void deleteTempTargetS3Bucket() throws IOException {
    S3Client.deleteBucket(s3TargetBucketName);
    BeforeActions.scenario.write("Deleted S3 Target Bucket " + s3TargetBucketName);
    s3TargetBucketName = StringUtils.EMPTY;
  }
}
