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
package io.cdap.plugin.amazons3.actions;

import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.amazons3.locators.AmazonS3Locators;
import org.openqa.selenium.By;

/**
 * StepActions for AmazonS3.
 */
public class AmazonS3Actions {

  static {
    SeleniumHelper.getPropertiesLocators(AmazonS3Locators.class);
  }

  public static void selectS3() {
    SeleniumHelper.waitAndClick(AmazonS3Locators.amazonS3Bucket);
  }

  public static void clickAmazonProperties() {
    AmazonS3Locators.amazonS3Properties.click();
  }

  public static void enterReferenceName(String referenceName) {
    AmazonS3Locators.referenceName.sendKeys(referenceName);
  }

  public static void closeButton() {
    AmazonS3Locators.closeButton.click();
  }

  public static void enterBucketPath(String recursiveReadPath) {
    AmazonS3Locators.amazonBucketPath.sendKeys(recursiveReadPath);
  }

  public static void readFilesRecursively() {
    AmazonS3Locators.readFilesRecursively.click();
  }

  public static void allowEmptyInput() {
    AmazonS3Locators.allowEmptyInput.click();
  }

  public static void fileEncoding(String encoding) throws InterruptedException {
    AmazonS3Locators.fileEncoding.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(By.xpath(
      "//*[contains(text(),'" + encoding + "')]")));
    CdfStudioActions.clickValidateButton();
  }

  public static void clickPreviewData() {
    SeleniumHelper.waitElementIsVisible(AmazonS3Locators.previewData);
    AmazonS3Locators.previewData.click();
  }

  public static void sinkAmazon() {
    AmazonS3Locators.sink.click();
    AmazonS3Locators.amazon.click();
  }

  public static void amazonProperties() {
    AmazonS3Locators.amazonProperties.click();
  }

  public static void selectFormat(String formatType) throws InterruptedException {
    AmazonS3Locators.format.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement
      (By.xpath("//li[@data-value='" + formatType + "']")));
  }

  public static void accessID(String accessID) {
    AmazonS3Locators.accessID.sendKeys(accessID);
  }

  public static void accessKey(String accessKey) {
    AmazonS3Locators.accessKey.sendKeys(accessKey);
  }

  public static void enterPathField(String fieldInOutputSchema) {
    AmazonS3Locators.pathField.sendKeys(fieldInOutputSchema);
  }

  public static void enterRegexFilter(String regexFilter) {
    AmazonS3Locators.regexPathFilter.sendKeys(regexFilter);
  }

  public static void enterMaxSplitSize(String splitsize) {
    AmazonS3Locators.maximumSplitSize.sendKeys(splitsize);
  }

  public static void clickIAM() {
    AmazonS3Locators.authenticationMethodIAM.click();
  }

  public static void selectPathFilename() {
    AmazonS3Locators.pathFileName.click();
  }

  public static void toggleSkipHeader() {
    AmazonS3Locators.skipHeader.click();
  }
}
