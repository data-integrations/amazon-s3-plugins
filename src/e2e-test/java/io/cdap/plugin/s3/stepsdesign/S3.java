package io.cdap.plugin.s3.stepsdesign;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.plugin.S3Client;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

/**
 * Amazon S3 Plugin related step design.
 */
public class S3 implements CdfHelper {

  @Then("Open Amazon S3 source properties")
  public void openS3SourceProperties() {
    openSourcePluginProperties("S3");
  }

  @Then("Open Amazon S3 sink properties")
  public void openS3SinkProperties() {
    openSinkPluginProperties("S3");
  }

  @Then("Verify data is transferred to target S3 bucket")
  public void verifyDataIsTransferredToTargetS3Bucket() {
    String s3Bucket = TestSetupHooks.s3TargetBucketName;
    boolean flag = false;
    try {
      for (S3ObjectSummary s3ObjectSummary : S3Client.listObjects(s3Bucket)) {
        if (s3ObjectSummary.getKey().contains("part")) {
          flag = true;
          break;
        }
      }
      if (flag) {
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
  
  @Then("Click on preview data for S3 sink")
  public void clickOnPreviewDataForS3Sink() {
    openSinkPluginPreviewData("S3");
  }
}
