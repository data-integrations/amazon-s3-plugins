package io.cdap.plugin.common.stepsdesign;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.S3Client;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Objects;
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

  public static void verifyDataIsTransferredToTargetS3Bucket() {
    String s3Bucket = TestSetupHooks.s3TargetBucketName;
    boolean flag = false;
    StringBuilder sb = new StringBuilder();
    try {
      for (S3ObjectSummary s3ObjectSummary : S3Client.listObjects(s3Bucket)) {
        if (s3ObjectSummary.getKey().contains("part")) {
          flag = true;
          String content = S3Client.getObject(s3Bucket, s3ObjectSummary.getKey());
          sb.append(content);
        }
      }
      if (flag) {
        String path = Paths.get(Objects.requireNonNull(TestSetupHooks.class.getResource
          ("/" + PluginPropertyUtils.pluginProp("outputCSV"))).getPath()).toString();
        Assert.assertEquals("Output content should match", readFile(path), sb.toString());
        BeforeActions.scenario.write("Data transferred to s3 bucket " + s3Bucket + " successfully");
      } else {
        Assert.fail("Data not transferred to target s3 bucket " + s3Bucket);
      }
    } catch (Exception e) {
      if (e.getMessage().contains("The specified bucket does not exist")) {
        Assert.fail("Target s3 bucket " + s3Bucket + " not created - " + e.getMessage());
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  private static String readFile(String path) throws IOException {
    File file = new File(path);
    FileInputStream fileInputStream = new FileInputStream(file);
    byte[] data = new byte[(int) file.length()];
    fileInputStream.read(data);
    fileInputStream.close();
    return new String(data, StandardCharsets.UTF_8);
  }
}
