#
# Copyright © 2023 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

@S3
Feature: S3 source - Design Time validation for S3 source

  Scenario:Validate error message providing incorrect credentials when verify credentials is true
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Amazon S3" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "Amazon S3"
    Then Enter input plugin property: "accessID" with value: "invalidAccessID"
    Then Enter input plugin property: "accessKey" with value: "invalidAccessKey"
    Then Enter input plugin property: "sessionToken" with value: "invalidSessionToken"
    Then Enter input plugin property: "referenceName" with value: "s3Source"
    Then Enter input plugin property: "path" with value: "testSourcePath"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Select radio button plugin property: "verifyCredentials" with value: "true"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidCredentials" on the header

  Scenario:Validate 'No errors found' message is displayed on providing incorrect credentials when verify credentials is false
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Amazon S3" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "Amazon S3"
    Then Enter input plugin property: "accessID" with value: "invalidAccessID"
    Then Enter input plugin property: "accessKey" with value: "invalidAccessKey"
    Then Enter input plugin property: "sessionToken" with value: "invalidSessionToken"
    Then Enter input plugin property: "referenceName" with value: "s3Source"
    Then Enter input plugin property: "path" with value: "testSourcePath"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Select radio button plugin property: "verifyCredentials" with value: "false"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Validate "Amazon S3" plugin properties
