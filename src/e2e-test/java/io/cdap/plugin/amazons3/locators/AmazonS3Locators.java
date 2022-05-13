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
package io.cdap.plugin.amazons3.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;
/**
 * StepLocators for AmazonS3.
 */
public class AmazonS3Locators {

  @FindBy(how = How.XPATH, using = "//*[@title=\"Amazon S3\"]//following-sibling::div")
  public static WebElement amazonS3Properties;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-S3-batchsource']")
  public static WebElement amazonS3Bucket;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='path']")
  public static WebElement amazonBucketPath;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-properties-validate-btn']")
  public static WebElement validate;

  @FindBy(how = How.XPATH, using = "//*[@class='fa fa-remove']")
  public static WebElement closeButton;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='maxSplitSize']")
  public static WebElement maximumSplitSize;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='fileRegex']")
  public static WebElement regexPathFilter;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='referenceName' and @class='MuiInputBase-input']")
  public static WebElement referenceName;

  @FindBy(how = How.XPATH, using = "//*[@name='filenameOnly' and@value='true']")
  public static WebElement pathFileName;

  @FindBy(how = How.XPATH, using = "//*[@name='recursive' and@value='true']")
  public static WebElement readFilesRecursively;

  @FindBy(how = How.XPATH, using = "//*[@name='ignoreNonExistingFolders' and@value='true']")
  public static WebElement allowEmptyInput;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='select-fileEncoding']")
  public static WebElement fileEncoding;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='S3-preview-data-btn' and @class='node-preview-data-btn ng-scope']")
  public static WebElement previewData;

  @FindBy(how = How.XPATH, using = "//*[text()='Sink ']")
  public static WebElement sink;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-S3-batchsink']")
  public static WebElement amazon;

  @FindBy(how = How.XPATH, using = "//*[@title=\"Amazon S3\"]//following-sibling::div")
  public static WebElement amazonProperties;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='accessID']")
  public static WebElement accessID;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='accessKey']")
  public static WebElement accessKey;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"select-format\"]")
  public static WebElement format;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='pathField']//input")
  public static WebElement pathField;

  @FindBy(how = How.XPATH, using = "//input[@value='IAM']")
  public static WebElement authenticationMethodIAM;

  @FindBy(how = How.XPATH, using = "//*[@class='MuiInputBase-input' and @data-cy='skipHeader']")
  public static WebElement skipHeader;
}
