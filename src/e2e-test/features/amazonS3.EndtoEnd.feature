Feature: AmazonS3 End to End records transfer

  @AmazonS3
  Scenario:Validate successful records transfer from Amazon to GCS
    Given Open Datafusion Project to configure pipeline
    When Source is Amazon
    When Sink is GCS
    Then Open Amazon Properties
    Then Enter the Amazon properties for bucket "amazonPath"
    Then Capture output schema
    Then Validate Amazon properties
    Then Close the Amazon properties
    Then Enter the GCS properties
    Then Close the GCS Properties
    Then Connect Source as "Amazon" and sink as "GCS" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for Amazon
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open the Logs and capture raw logs
    Then Validate the output record count

  @AmazonS3
  Scenario:Validate successful records transfer from Amazon to BigQuery using authentication method as IAM
    Given Open Datafusion Project to configure pipeline
    Given Delete the table "amazonBqTableDemo"
    When Source is Amazon
    When Target is BigQuery
    Then Open Amazon Properties
    Then Enter the Amazon properties using IAM for bucket "amazonPath"
    Then Capture output schema
    Then Validate Amazon properties
    Then Close the Amazon properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery properties for table "amazonBqTableDemo"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "Amazon" and sink as "BigQuery" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for Amazon
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate the output record count
    Then Open the Logs and capture raw logs
    Then Get Count of no of records transferred to BigQuery in "amazonBqTableDemo"
    Then Validate records out from Amazon is equal to records transferred in BigQuery "amazonBqTableDemo" output records

  @AmazonS3
  Scenario:Validate successful records transfer from Amazon to BigQuery when Path Filename Only is set to true
    Given Open Datafusion Project to configure pipeline
    When Source is Amazon
    When Target is BigQuery
    Then Open Amazon Properties
    Then Enter the Amazon properties for bucket "amazonPath" and path field "amazonPathField" with pathFileName only set "True"
    Then Capture output schema
    Then Validate Amazon properties
    Then Close the Amazon properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery properties for table "amazonBqTableDemo"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "Amazon" and sink as "BigQuery" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for Amazon
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate the output record count
    Then Open the Logs and capture raw logs
    Then Get Count of no of records transferred to BigQuery in "amazonBqTableDemo"
    Then Validate records out from Amazon is equal to records transferred in BigQuery "amazonBqTableDemo" output records
    Then Delete the table "amazonBqTableDemo"

  @AmazonS3
  Scenario:Validate successful records transfer from Amazon to BigQuery when Path Filename Only is set to False
    Given Open Datafusion Project to configure pipeline
    When Source is Amazon
    When Target is BigQuery
    Then Open Amazon Properties
    Then Enter the Amazon properties for bucket "amazonPath" and path field "amazonPathField" with pathFileName only set "False"
    Then Capture output schema
    Then Validate Amazon properties
    Then Close the Amazon properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery properties for table "amazonBqTableDemo"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "Amazon" and sink as "BigQuery" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for Amazon
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate the output record count
    Then Get Count of no of records transferred to BigQuery in "amazonBqTableDemo"
    Then Validate records out from Amazon is equal to records transferred in BigQuery "amazonBqTableDemo" output records
    Then Delete the table "amazonBqTableDemo"

  @AmazonS3
  Scenario:Validate successful records transfer from Amazon to BigQuery when Read Files Recursively is set to true
    Given Open Datafusion Project to configure pipeline
    When Source is Amazon
    When Target is BigQuery
    Then Open Amazon Properties
    Then Enter the Amazon properties for bucket "amazonRecursiveDataPath" and field readFilesRecursively
    Then Capture output schema
    Then Validate Amazon properties
    Then Close the Amazon properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery properties for table "amazonBqTableDemo"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "Amazon" and sink as "BigQuery" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for Amazon
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate the output record count
    Then Get Count of no of records transferred to BigQuery in "amazonBqTableDemo"
    Then Validate records out from Amazon is equal to records transferred in BigQuery "amazonBqTableDemo" output records
    Then Delete the table "amazonBqTableDemo"

  @AmazonS3
  Scenario: Verify records transfer from AmazonS3 to BigQuery on using Regex path filter
    Given Open Datafusion Project to configure pipeline
    When Source is Amazon
    When Target is BigQuery
    Then Open Amazon Properties
    Then Enter the Amazon properties for bucket "amazonRegexPath" and regex filter "amazonRegexFilter"
    Then Capture output schema
    Then Validate Amazon properties
    Then Close the BigQuery properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery properties for table "amazonBqTableDemo"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "Amazon" and sink as "BigQuery" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for BigQuery
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate the output record count
    Then Get Count of no of records transferred to BigQuery in "amazonBqTableDemo"
    Then Validate records out from Amazon is equal to records transferred in BigQuery "amazonBqTableDemo" output records
    Then Delete the table "amazonBqTableDemo"

  @AmazonS3
  Scenario Outline: Verify output records are encoded while transferring data from Amazon to BigQuery using different file encoding
    Given Open Datafusion Project to configure pipeline
    When Source is Amazon
    When Target is BigQuery
    Then Open Amazon Properties
    Then Enter the Amazon properties for bucket "amazonPath" and selecting "<amazonDifferentEncodings>" File encoding
    Then Capture output schema
    Then Validate Amazon properties
    Then Close the Amazon properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery properties for table "amazonBqTableDemo"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "Amazon" and sink as "BigQuery" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for Amazon
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate the output record count
    Then Get Count of no of records transferred to BigQuery in "amazonBqTableDemo"
    Then Validate records out from Amazon is equal to records transferred in BigQuery "amazonBqTableDemo" output records
    Then Delete the table "amazonBqTableDemo"
    Examples:
      | amazonDifferentEncodings                |
      | amazonLatinEncoding                     |
      | amazonEuropeanEncoding                  |
      | amazonTurkishEncoding                   |

  @AmazonS3
  Scenario:Validate successful records transfer from BigQuery to Amazon as sink
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is Amazon
    Then Open BigQuery Properties
    Then Enter the BigQuery properties for source table "amazonBqTableName"
    Then Capture output schema
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Open Amazon Target Properties
    Then Enter the Amazon properties for sink bucket "amazonSinkBucket"
    Then Validate Amazon properties
    Then Close the Amazon properties
    Then Connect Source as "BigQuery" and sink as "Amazon" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for BigQuery
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open the Logs and capture raw logs
    Then Validate the output record count
