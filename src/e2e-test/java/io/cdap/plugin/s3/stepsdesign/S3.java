package io.cdap.plugin.s3.stepsdesign;

import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.S3Client;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

  @Then("Verify csv data is transferred to target S3 bucket")
  public void verifyCSVDataIsTransferredToTargetS3Bucket() {
    String s3Bucket = TestSetupHooks.s3TargetBucketName;
    boolean flag = false;
    List<String> lst = new ArrayList<>();
    try {
      for (S3ObjectSummary s3ObjectSummary : S3Client.listObjects(s3Bucket)) {
        if (s3ObjectSummary.getKey().contains("part")) {
          flag = true;
          try (S3ObjectInputStream inputStream = S3Client.getObjectContent(s3Bucket, s3ObjectSummary.getKey())) {
            readInputStream(inputStream, lst);
          }
        }
      }
      if (flag) {
        Path path = Paths.get(Objects.requireNonNull(S3.class.getResource
          ("/" + PluginPropertyUtils.pluginProp("outputCSV"))).getPath()).toAbsolutePath();
        Assert.assertEquals("Output content should match",
                            new String(Files.readAllBytes(path)), getSortedCSVContent(lst));
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

  private String getSortedCSVContent(List<String> lst) {
    // Since the spark output files aren't guaranteed to be ordered, the output entry with [id, first_name] schema needs
    // to be sorted by id for comparison purpose.
    lst.sort((s1, s2) -> {
      String id1 = s1.split(",")[0], id2 = s2.split(",")[0];
      return Integer.parseInt(id1) - Integer.parseInt(id2);
    });
    StringBuilder sb = new StringBuilder();
    for (String s : lst) {
      sb.append(s);
    }
    return sb.toString();
  }

  private void readInputStream(InputStream input, List<String> lst) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lst.add(line + "\n");
      }
    }
  }

  @Then("Click on preview data for S3 sink")
  public void clickOnPreviewDataForS3Sink() {
    openSinkPluginPreviewData("S3");
  }
}
