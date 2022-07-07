package io.cdap.plugin.s3.stepsdesign;

import io.cdap.e2e.utils.CdfHelper;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cucumber.java.en.Then;

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
    TestSetupHooks.verifyDataIsTransferredToTargetS3Bucket();
  }
  
  @Then("Click on preview data for S3 sink")
  public void clickOnPreviewDataForS3Sink() {
    openSinkPluginPreviewData("S3");
  }
}
