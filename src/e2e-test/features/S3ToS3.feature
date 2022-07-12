@S3
Feature: S3 source - Verification of S3 to S3 successful data transfer
  @S3_SOURCE_TEST @S3_SINK_TEST
  Scenario:Validate successful records transfer from S3 to S3
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Amazon S3" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Amazon S3" from the plugins list as: "Sink"
    Then Connect source as "Amazon-S3" and sink as "S3" to establish connection
    Then Open Amazon S3 source properties
    Then Enter input plugin property: "accessID" with value: "accessID"
    Then Enter input plugin property: "accessKey" with value: "accessKey"
    Then Enter input plugin property: "referenceName" with value: "s3Source"
    Then Enter input plugin property: "path" with value: "sourcePath"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "outputSchema"
    Then Validate "S3" plugin properties
    Then Close the Plugin Properties page
    Then Open Amazon S3 sink properties
    Then Enter input plugin property: "accessID" with value: "accessID"
    Then Enter input plugin property: "accessKey" with value: "accessKey"
    Then Enter input plugin property: "referenceName" with value: "s3Sink"
    Then Enter input plugin property: "path" with value: "sinkPath"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Validate "S3" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on preview data for S3 sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify csv data is transferred to target S3 bucket
