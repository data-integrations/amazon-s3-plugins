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
 */
package io.cdap.plugin.amazons3.stepsdesign;

import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfLogActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfBigQueryPropertiesLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.GcpClient;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.amazons3.actions.AmazonS3Actions;
import io.cdap.plugin.amazons3.locators.AmazonS3Locators;
import io.cdap.plugin.utils.E2ETestUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import stepsdesign.BeforeActions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static io.cdap.plugin.utils.E2ETestConstants.ERROR_MSG_AMAZON_INVALID_PATH;
import static io.cdap.plugin.utils.E2ETestConstants.ERROR_MSG_AMAZON_INVALID_PATH_FIELD;
import static io.cdap.plugin.utils.E2ETestConstants.ERROR_MSG_COLOR;
import static io.cdap.plugin.utils.E2ETestConstants.ERROR_MSG_ERROR_FOUND_VALIDATION;
import static io.cdap.plugin.utils.E2ETestConstants.ERROR_MSG_INCORRECT_TABLE;
import static io.cdap.plugin.utils.E2ETestConstants.ERROR_MSG_VALIDATION;

/**
 * StepDesigns for AmazonS3.
 */
public class AmazonS3 implements CdfHelper {
  GcpClient gcpClient = new GcpClient();

  static PrintWriter out;
  static String rawLog;
  static int countRecords;
  List<String> propertiesOutputSchema = new ArrayList<String>();

  static {
    try {
      out = new PrintWriter(BeforeActions.myObj);
    } catch (FileNotFoundException e) {
      BeforeActions.scenario.write(e.toString());
    }
  }

  @When("Source is Amazon")
  public void sourceIsAmazon() throws InterruptedException {
    AmazonS3Actions.selectS3();
  }

  @When("Target is BigQuery")
  public void targetIsBigQuery() {
    CdfStudioActions.sinkBigQuery();

  }

  @Given("Open Datafusion Project to configure pipeline")
  public void openDatafusionProjectToConfigurePipeline() throws IOException, InterruptedException {
    openCdf();
  }

  @When("Sink is GCS")
  public void sinkIsGCS() {
    CdfStudioActions.sinkGcs();
  }

  @Then("Close the Amazon properties")
  public void closeTheAmazonProperties() {
    AmazonS3Actions.closeButton();
  }

  @Then("Enter the GCS properties")
  public void enterTheGCSProperties() throws InterruptedException, IOException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterProjectId();
    CdfGcsActions.getGcsBucket(E2ETestUtils.pluginProp("amazonGcsBucketName"));
    CdfGcsActions.selectFormat("json");
    CdfGcsActions.clickValidateButton();
  }

  @Then("Add pipeline name")
  public void addPipelineName() throws InterruptedException {
    CdfStudioActions.pipelineName();
    CdfStudioActions.pipelineNameIp("Amazon_GCS" + UUID.randomUUID().toString());
    CdfStudioActions.pipelineSave();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.statusBanner);
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 5);
    wait.until(ExpectedConditions.invisibilityOf(CdfStudioLocators.statusBanner));
  }

  @Then("Click the preview")
  public void clickThePreview() {
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.preview, 400);
    CdfStudioLocators.preview.click();
  }

  @Then("Close the GCS Properties")
  public void closeTheGCSProperties() {
    CdfGcsActions.closeButton();
  }

  @Then("Run the Pipeline in Runtime")
  public void runThePipelineInRuntime() throws InterruptedException {
    CdfPipelineRunAction.runClick();
  }

  @Then("Wait till pipeline is in running state")
  public void waitTillPipelineIsInRunningState() throws InterruptedException {
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 500);
    wait.until(ExpectedConditions.or(
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Succeeded']")),
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Failed']"))));
  }

  @Then("Verify the pipeline status is {string}")
  public void verifyThePipelineStatusIs(String status) {
    Assert.assertTrue(SeleniumHelper.verifyElementPresent("//*[@data-cy='" + status + "']"));
  }

  @Then("Click on Advance logs and validate the success message")
  public void clickOnAdvanceLogsAndValidateTheSuccessMessage() {
    CdfLogActions.goToAdvanceLogs();
    CdfLogActions.validateSucceeded();
  }

  @Then("Validate Amazon properties")
  public void validateAmazonPropertiesForErrorWithoutProvidingMandatoryFields() {
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton);
    String expectedErrorMessage = E2ETestUtils.errorProp(ERROR_MSG_VALIDATION);
    String actualErrorMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
  }

  @Then("Enter the Amazon Properties with mandatory and basic fields")
  public void enterTheAmazonProperties() throws InterruptedException, IOException {
    AmazonS3Actions.clickAmazonProperties();
    AmazonS3Actions.accessID(System.getenv("AWS_KEY_ID"));
    AmazonS3Actions.accessKey(System.getenv("AWS_SECRET_KEY"));
    AmazonS3Actions.enterReferenceName(E2ETestUtils.pluginProp("referenceName"));
    AmazonS3Actions.enterBucketPath(E2ETestUtils.pluginProp("bucketPath"));
    CdfBigQueryPropertiesActions.getSchema();
    CdfGcsActions.clickValidateButton();
  }

  @Then("Open Amazon Properties")
  public void openAmazonProperties() {
    CdfStudioActions.clickProperties("Amazon");
  }

  @Then("Enter the Amazon properties for bucket {string}")
  public void enterTheAmazonPropertiesForBucket(String path) {
    AmazonS3Actions.enterBucketPath(E2ETestUtils.pluginProp(path));
    AmazonS3Actions.enterReferenceName("Amazon" + UUID.randomUUID().toString());
    AmazonS3Actions.accessID(System.getenv("AWS_KEY_ID"));
    AmazonS3Actions.accessKey(System.getenv("AWS_SECRET_KEY"));
  }

  @Then("Enter the Amazon properties for sink bucket {string}")
  public void enterTheAmazonPropertiesForSinkBucket(String path) {
    AmazonS3Actions.enterBucketPath(E2ETestUtils.pluginProp(path));
    AmazonS3Actions.enterReferenceName("Amazon" + UUID.randomUUID().toString());
    AmazonS3Actions.accessID(System.getenv("AWS_KEY_ID"));
    AmazonS3Actions.accessKey(System.getenv("AWS_SECRET_KEY"));
  }

  @Then("Capture output schema")
  public void captureOutputSchema() {
    CdfBigQueryPropertiesActions.getSchema();
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 10);
    wait.until(ExpectedConditions.numberOfElementsToBeMoreThan
      (By.xpath("//*[@placeholder=\"Field name\"]"), 1));
    SeleniumHelper.waitElementIsVisible(SeleniumDriver.getDriver().findElement(
      By.xpath("//div[@data-cy='schema-fields-list']//*[@placeholder='Field name']")), 10L);
    List<WebElement> propertiesOutputSchemaElements = SeleniumDriver.getDriver().findElements(
      By.xpath("//div[@data-cy='schema-fields-list']//*[@placeholder='Field name']"));
    for (WebElement element : propertiesOutputSchemaElements) {
      propertiesOutputSchema.add(element.getAttribute("value"));
    }
    Assert.assertTrue(propertiesOutputSchema.size() >= 2);
  }

  @Then("Connect Source as {string} and sink as {string} to establish connection")
  public void connectSourceAsAndSinkAsToEstablishConnection(String source, String sink) {
    CdfStudioActions.connectSourceAndSink(source, sink);
  }

  @Then("Preview and run the pipeline")
  public void previewAndRunThePipeline() {
    SeleniumHelper.waitAndClick(CdfStudioLocators.preview, 5L);
    CdfStudioLocators.runButton.click();
  }

  @Then("Verify the preview of pipeline is {string}")
  public void verifyThePreviewOfPipelineIs(String previewStatus) {
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 180);
    wait.until(ExpectedConditions.visibilityOf(CdfStudioLocators.statusBanner));
    Assert.assertTrue(CdfStudioLocators.statusBannerText.getText().contains(previewStatus));
    if (!previewStatus.equalsIgnoreCase("failed")) {
      wait.until(ExpectedConditions.invisibilityOf(CdfStudioLocators.statusBanner));
    }
  }

  @Then("Click on PreviewData for Amazon")
  public void clickOnPreviewDataForAmazon() {
    AmazonS3Actions.clickPreviewData();
  }

  @Then("Verify Preview output schema matches the outputSchema captured in properties")
  public void verifyPreviewOutputSchemaMatchesTheOutputSchemaCapturedInProperties() {
    List<String> previewOutputSchema = new ArrayList<String>();
    List<WebElement> previewOutputSchemaElements = SeleniumDriver.getDriver().findElements(
      By.xpath("(//h2[text()='Output Records']/parent::div/div/div/div/div)[1]//div[text()!='']"));
    for (WebElement element : previewOutputSchemaElements) {
      previewOutputSchema.add(element.getAttribute("title"));
    }
    Assert.assertTrue(previewOutputSchema.equals(propertiesOutputSchema));

  }

  @Then("Close the Preview and deploy the pipeline")
  public void closeThePreviewAndDeployThePipeline() {
    SeleniumHelper.waitAndClick(CdfStudioLocators.closeButton, 5L);
    CdfStudioActions.previewSelect();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pipelineDeploy, 2);
    CdfStudioActions.pipelineDeploy();
  }

  @Then("Open the Logs and capture raw logs")
  public void openTheLogsAndCaptureRawLogs() {
    CdfPipelineRunAction.logsClick();
    rawLog = CdfPipelineRunAction.captureRawLogs();
    SeleniumDriver.getDriver().navigate().refresh();
    BeforeActions.scenario.write(rawLog);
    out.println(rawLog);
    out.close();
  }

  @Then("Validate successMessage is displayed when pipeline is succeeded")
  public void validateSuccessMessageIsDisplayedWhenPipelineIsSucceeded() {
    CdfLogActions.validateSucceeded();
  }

  @Then("Validate the output record count")
  public void validateTheOutputRecordCount() {
    Assert.assertTrue(recordOut() > 0);
  }

  @Then("Open BigQuery Target Properties")
  public void openBigQueryTargetProperties() {
    CdfStudioActions.clickProperties("BigQuery");
  }

  @Then("Enter the BigQuery properties for table {string}")
  public void entertheBigQuerypropertiesfortable(String tableName) throws IOException {
    enterTheBigQueryPropertiesForTable(tableName);
    CdfBigQueryPropertiesActions.clickUpdateTable();
    CdfBigQueryPropertiesActions.clickTruncatableSwitch();
  }

  private void enterTheBigQueryPropertiesForTable(String tableName) throws IOException {
    CdfBigQueryPropertiesActions.enterProjectId(E2ETestUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterDatasetProjectId(E2ETestUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterBigQueryReferenceName("BQ_Ref_" + UUID.randomUUID().toString());
    CdfBigQueryPropertiesActions.enterBigQueryDataset(E2ETestUtils.pluginProp("dataset"));
    CdfBigQueryPropertiesActions.enterBigQueryTable(E2ETestUtils.pluginProp(tableName));
  }

  @Then("Validate Bigquery properties")
  public void validateBigqueryProperties() {
    CdfGcsActions.clickValidateButton();
    String expectedErrorMessage = E2ETestUtils.errorProp(ERROR_MSG_VALIDATION);
    String actualErrorMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
  }

  @Then("Click on PreviewData for BigQuery")
  public void clickOnPreviewDataForBigQuery() {
    CdfBigQueryPropertiesActions.clickPreviewData();
  }

  @Then("Validate record transferred from Amazon {string} is equal to BigQuery {string} output records")
  public void validateRecordTransferredFromAmazonIsEqualToBigQueryOutputRecords
    (String table, String field) throws IOException, InterruptedException {
    String projectId = (E2ETestUtils.pluginProp("projectId"));
    String datasetName = (E2ETestUtils.pluginProp("dataset"));
    String selectQuery = "SELECT count(*)  FROM `" + projectId + "." + datasetName + "." + E2ETestUtils.pluginProp
      (table) + "` WHERE " +
      E2ETestUtils.pluginProp(field);
    int count = GcpClient.executeQuery(selectQuery);
    BeforeActions.scenario.write("number of records transferred with respect to filter:"
                                   + count);
    Assert.assertEquals(count, countRecords);
  }

  @Then("Enter the Amazon Properties with blank property {string}")
  public void enterTheAmazonPropertiesWithBlankProperty(String property) {
    if (property.equalsIgnoreCase("referenceName")) {
      AmazonS3Actions.enterBucketPath(E2ETestUtils.pluginProp("amazonPath"));
    } else if (property.equalsIgnoreCase("amazonPath")) {
      AmazonS3Actions.enterReferenceName("Amazon_" + UUID.randomUUID().toString());
    }
  }

  @Then("Validate mandatory property error for {string}")
  public void validateMandatoryPropertyErrorFor(String property) {
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton, 5L);
    E2ETestUtils.validateMandatoryPropertyError(property);
  }

  @Then("Get Count of no of records transferred to BigQuery in {string}")
  public void getCountOfNoOfRecordsTransferredToBigQueryIn(String tableName) throws IOException, InterruptedException {
    int countRecords;
    countRecords = GcpClient.countBqQuery(E2ETestUtils.pluginProp(tableName));
    BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
    Assert.assertTrue(countRecords > 0);
  }

  @Then("Delete the table {string}")
  public void deleteTheTable(String table) throws IOException, InterruptedException {
    try {
      int existingRecords = GcpClient.countBqQuery(E2ETestUtils.pluginProp(table));
      if (existingRecords > 0) {
        GcpClient.dropBqQuery(E2ETestUtils.pluginProp(table));
        BeforeActions.scenario.write("Table Deleted Successfully");
      }
    } catch (Exception e) {
      BeforeActions.scenario.write(e.toString());
    }
  }

  @Then("Enter the Amazon Properties with incorrect property {string} value {string}")
  public void enterTheAmazonPropertiesWithIncorrectPropertyValue(String field, String value) {
    if (field.equalsIgnoreCase("path")) {
      AmazonS3Actions.enterBucketPath(E2ETestUtils.pluginProp(value));
      AmazonS3Actions.enterReferenceName("Amazon" + UUID.randomUUID().toString());
    } else if (field.equalsIgnoreCase("Maximum Split Size")) {
      SeleniumHelper.replaceElementValue(AmazonS3Locators.maximumSplitSize, E2ETestUtils.pluginProp(value));
      AmazonS3Actions.enterBucketPath(E2ETestUtils.pluginProp("amazonPath"));
      AmazonS3Actions.enterReferenceName("Amazon" + UUID.randomUUID().toString());
    } else if (field.equalsIgnoreCase("Path Field ")) {
      SeleniumHelper.replaceElementValue(AmazonS3Locators.pathField, E2ETestUtils.pluginProp(value));
      AmazonS3Actions.enterBucketPath(E2ETestUtils.pluginProp("amazonPath"));
      AmazonS3Actions.enterReferenceName("Amazon" + UUID.randomUUID().toString());
    }
  }

  @Then("Validate incorrect property error for table {string} value {string}")
  public void validateIncorrectPropertyErrorForTableValue(String property, String value) {
    CdfBigQueryPropertiesActions.getSchema();
    SeleniumHelper.waitElementIsVisible(CdfBigQueryPropertiesLocators.getSchemaButton, 5L);
    String tableFullName = StringUtils.EMPTY;
    if (property.equalsIgnoreCase("dataset")) {
      tableFullName = E2ETestUtils.pluginProp("projectId") + ":" + E2ETestUtils.pluginProp(value)
        + "." + E2ETestUtils.pluginProp("amazonBqTableName");
    } else if (property.equalsIgnoreCase("table")) {
      tableFullName = E2ETestUtils.pluginProp("projectId") + ":" + E2ETestUtils.pluginProp("dataset")
        + "." + E2ETestUtils.pluginProp(value);
    } else if (property.equalsIgnoreCase("datasetProject")) {
      tableFullName = E2ETestUtils.pluginProp(value) + ":" + E2ETestUtils.pluginProp("dataset")
        + "." + E2ETestUtils.pluginProp("amazonBqTableName");
    }
    String expectedErrorMessage = E2ETestUtils.errorProp(ERROR_MSG_INCORRECT_TABLE)
      .replaceAll("TABLENAME", tableFullName);
    String actualErrorMessage = E2ETestUtils.findPropertyErrorElement("table").getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = E2ETestUtils.getErrorColor(E2ETestUtils.findPropertyErrorElement("table"));
    String expectedColor = E2ETestUtils.errorProp(ERROR_MSG_COLOR);
    Assert.assertEquals(expectedColor, actualColor);
  }

  @Then("Enter the Amazon Properties for bucket {string} and format {string} with delimiter field {string}")
  public void enterTheAmazonPropertiesForBucketAndFormatWithDelimiterField
    (String bucket, String format, String delimiter) throws InterruptedException {
    enterTheAmazonPropertiesForBucket(bucket);
    AmazonS3Actions.selectFormat(E2ETestUtils.pluginProp(format));
    CdfGcsActions.enterDelimiterField(E2ETestUtils.pluginProp(delimiter));
  }

  @Then("Enter the Amazon properties for bucket {string} and path field {string} with pathFileName only set {string}")
  public void enterTheAmazonPropertiesForBucketAndPathFieldWithPathFileNameOnlySet
    (String path, String field, String pathNameOnly) {
    enterTheAmazonPropertiesForBucket(path);
    AmazonS3Actions.enterPathField(E2ETestUtils.pluginProp(field));
    if (pathNameOnly.equalsIgnoreCase("true")) {
      AmazonS3Actions.selectPathFilename();
    }
  }

  @Then("Enter the Amazon properties for emptyvaluepath {string}")
  public void enterTheAmazonPropertiesForEmptyvaluepath(String path) {
    enterTheAmazonPropertiesForBucket(path);
    AmazonS3Actions.allowEmptyInput();
  }

  @Then("Validate the output record count is equal to zero")
  public void validateTheOutputRecordCountIsEqualToZero() {
    Assert.assertTrue(recordOut() == 0);

  }

  @Then("Enter the Amazon properties for bucket {string} and field readFilesRecursively")
  public void enterTheAmazonPropertiesForBucketAndField(String path) {
    enterTheAmazonPropertiesForBucket(path);
    AmazonS3Actions.readFilesRecursively();
  }

  @Then("Enter the Amazon properties for bucket {string} and regex filter {string}")
  public void enterTheAmazonPropertiesForBucketAndRegexFilter(String path, String regexFilter) {
    enterTheAmazonPropertiesForBucket(path);
    AmazonS3Actions.enterRegexFilter(E2ETestUtils.pluginProp(regexFilter));
  }

  @Then("Verify plugin properties validation fails with error")
  public void verifyPluginPropertiesValidationFailsWithError() {
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton, 5L);
    String expectedErrorMessage = E2ETestUtils.errorProp(ERROR_MSG_ERROR_FOUND_VALIDATION);
    String actualErrorMessage = CdfStudioLocators.pluginValidationErrorMsg.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
  }

  @Then("Verify invalid amazon bucket path error")
  public void verifyInvalidAmazonBucketPathError() {
    CdfStudioActions.clickValidateButton();
    String expectedErrorMessage = E2ETestUtils.errorProp(ERROR_MSG_AMAZON_INVALID_PATH);
    String actualErrorMessage = E2ETestUtils.findPropertyErrorElement("path").getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = E2ETestUtils.getErrorColor(E2ETestUtils.findPropertyErrorElement("path"));
    String expectedColor = E2ETestUtils.errorProp(ERROR_MSG_COLOR);
    Assert.assertEquals(expectedColor, actualColor);
  }

  @Then("Enter the Amazon properties with blank authentication fields {string}")
  public void enterTheAmazonPropertiesWithBlankAuthenticationFields(String authenticationMethod) {
    if (authenticationMethod.equalsIgnoreCase("accessID")) {
      AmazonS3Actions.enterBucketPath(E2ETestUtils.pluginProp("amazonRecursiveDataPath"));
      AmazonS3Actions.enterReferenceName("Amazon" + UUID.randomUUID().toString());
      AmazonS3Actions.accessKey(E2ETestUtils.pluginProp("accessKey"));
    } else if (authenticationMethod.equalsIgnoreCase("accessKey")) {
      AmazonS3Actions.enterBucketPath(E2ETestUtils.pluginProp("amazonRecursiveDataPath"));
      AmazonS3Actions.enterReferenceName("Amazon" + UUID.randomUUID().toString());
      AmazonS3Actions.accessID(E2ETestUtils.pluginProp("accessID"));
    }
  }

  @When("Target is Amazon")
  public void targetIsAmazon() {
    AmazonS3Actions.sinkAmazon();
  }

  @Then("Open BigQuery Properties")
  public void openBigQueryProperties() {
    CdfStudioActions.clickProperties("BigQuery");
  }

  @Then("Open Amazon Target Properties")
  public void openAmazonTargetProperties() {
    CdfStudioActions.clickProperties("Amazon");
    }

    @Then("Validate records out from Amazon is equal to records transferred in BigQuery {string} output records")
  public void validateRecordsOutFromAmazonIsEqualToRecordsTransferredInBigQueryOutputRecords(String tableName)
      throws IOException, InterruptedException {
    int countRecords;
    countRecords = gcpClient.countBqQuery(E2ETestUtils.pluginProp(tableName));
    Assert.assertEquals(countRecords, recordOut());
  }
  @Then("Verify output field {string} in target BigQuery table {string} contains path of the amzonbucket {string}")
  public void verifyOutputFieldInTargetBigQueryTableContainsPathOfTheAmzonbucket
    (String field, String targetTable, String bucketPath) throws IOException, InterruptedException {
    Optional<String> result = GcpClient
      .getSoleQueryResult("SELECT distinct " + E2ETestUtils.pluginProp(field) + " as bucket FROM `"
                            + (E2ETestUtils.pluginProp("projectId")) + "."
                            + (E2ETestUtils.pluginProp("dataset")) + "."
                            + E2ETestUtils.pluginProp(targetTable) + "` ");
    String pathFromBQTable = StringUtils.EMPTY;
    if (result.isPresent()) {
      pathFromBQTable = result.get();
    }
    BeforeActions.scenario.write("Amazon bucket path in BQ Table :" + pathFromBQTable);
    Assert.assertEquals(E2ETestUtils.pluginProp(bucketPath), pathFromBQTable);
  }

  @When("Source is BigQuery")
  public void sourceIsBigQuery() throws InterruptedException {
    CdfStudioActions.selectBQ();
  }

  @Then("Close the BigQuery properties")
  public void closeTheBigQueryProperties() {
  CdfStudioActions.clickCloseButton();
  }

  @Then("Enter the Amazon properties for bucket {string} and incorrect split size {string}")
  public void enterTheAmazonPropertiesForBucketAndIncorrectSplitSize(String path, String value) {
    enterTheAmazonPropertiesForBucket(path);
    AmazonS3Actions.enterMaxSplitSize(E2ETestUtils.pluginProp(value));
  }

  @Then("Verify invalid split size error")
  public void verifyInvalidSplitSizeError() {
    CdfStudioActions.clickValidateButton();
    String expectedErrorMessage = E2ETestUtils.errorProp(ERROR_MSG_ERROR_FOUND_VALIDATION);
    String actualErrorMessage = E2ETestUtils.findPropertyErrorElement("maxSplitSize").getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = E2ETestUtils.getErrorColor(E2ETestUtils.findPropertyErrorElement("maxSplitSize"));
    String expectedColor = E2ETestUtils.errorProp(ERROR_MSG_COLOR);
    Assert.assertEquals(expectedColor, actualColor);

  }

  @Then("Enter the Amazon properties using IAM for bucket {string}")
  public void enterTheAmazonPropertiesUsingIAMForBucket(String path) {
    AmazonS3Actions.enterBucketPath(E2ETestUtils.pluginProp(path));
    AmazonS3Actions.enterReferenceName("Amazon" + UUID.randomUUID().toString());
    AmazonS3Actions.clickIAM();
  }

  @Then("Enter the Amazon properties for bucket {string} and format type {string}")
  public void enterTheAmazonPropertiesForBucketAndFormatType(String path, String format) throws InterruptedException {
    enterTheAmazonPropertiesForBucket(path);
    AmazonS3Actions.selectFormat(E2ETestUtils.pluginProp(format));
  }

  @Then("Validate output error is not displayed")
  public void validateOutputErrorIsNotDisplayed() {
    Assert.assertFalse(SeleniumHelper.isElementPresent(CdfStudioLocators.pluginValidationErrorMsg));
  }

  @Then("Validate get schema is loaded without error")
  public void validateGetSchemaIsLoadedWithoutError() {
    SeleniumHelper.waitElementIsVisible(CdfBigQueryPropertiesLocators.getSchemaButton, 10);
  }

  @Then("Enter the Amazon properties using IAM for bucket {string} and format type {string}")
  public void enterTheAmazonPropertiesUsingIAMForBucketAndFormatType(String path, String format)
    throws InterruptedException {
    enterTheAmazonPropertiesForBucket(path);
    AmazonS3Actions.selectFormat(E2ETestUtils.pluginProp(format));
  }

  @Then("Enter the Amazon properties for bucket {string} and selecting {string} File encoding")
  public void enterTheAmazonPropertiesForBucketAndSelectingFileEncoding(String path, String encoding)
    throws InterruptedException {
    enterTheAmazonPropertiesForBucket(path);
    AmazonS3Actions.fileEncoding(E2ETestUtils.pluginProp(encoding));
  }

  @Then("Enter the BigQuery properties for source table {string}")
  public void enterTheBigQueryPropertiesForSourceTable(String table) throws IOException {
    enterTheBigQueryPropertiesForTable(table);
  }

  @Then("Enter the Amazon properties for bucket {string} and incorrect path field {string}")
  public void enterTheAmazonPropertiesForBucketAndIncorrectPathField(String path, String pathfield) {
    enterTheAmazonPropertiesForBucket(path);
    AmazonS3Actions.enterPathField(E2ETestUtils.pluginProp(pathfield));
  }

  @Then("Verify invalid path field error")
  public void verifyInvalidPathFieldError() {
    CdfStudioActions.clickValidateButton();
    String expectedErrorMessage = E2ETestUtils.errorProp(ERROR_MSG_AMAZON_INVALID_PATH_FIELD);
    String actualErrorMessage = E2ETestUtils.findPropertyErrorElement("pathField").getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = E2ETestUtils.getErrorColor(E2ETestUtils.findPropertyErrorElement("pathField"));
    String expectedColor = E2ETestUtils.errorProp(ERROR_MSG_COLOR);
    Assert.assertEquals(expectedColor, actualColor);
  }

  @Then("Enter the Amazon properties for bucket {string} and skip header value {string}")
  public void enterTheAmazonPropertiesForBucketAndSkipHeaderValue(String path, String arg1) {
    enterTheAmazonPropertiesForBucket(path);
    AmazonS3Actions.toggleSkipHeader();
  }

  @Then("Validate Skip header toggle button is selected")
  public void validateSkipHeaderToggleButtonIsSelected() {
   Assert.assertTrue(AmazonS3Locators.skipHeader.isSelected());
  }
}
