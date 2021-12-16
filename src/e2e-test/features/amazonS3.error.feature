Feature: AmazonS3 error validations

  @AmazonS3
  Scenario Outline:Verify Amazon Source properties validation errors for mandatory fields
  Given Open Datafusion Project to configure pipeline
  When Source is Amazon
  Then Open Amazon Properties
  Then Enter the Amazon Properties with blank property "<property>"
  Then Validate mandatory property error for "<property>"
  Examples:
  | property        |
  | referenceName   |
  | path            |

  @AmazonS3
  Scenario:Verify Error message for incorrect Amazon bucket path
  Given Open Datafusion Project to configure pipeline
  When Source is Amazon
  Then Open Amazon Properties
  Then Enter the Amazon properties for bucket "amazonIncorrectPath"
  Then Verify invalid amazon bucket path error

  @AmazonS3
  Scenario:Verify Error message for incorrect Maximum Split Size
  Given Open Datafusion Project to configure pipeline
  When Source is Amazon
  Then Open Amazon Properties
  Then Enter the Amazon properties for bucket "amazonPath" and incorrect split size "amazonIncorrectSplitSize"
  Then Verify invalid split size error

  @AmazonS3
  Scenario:Verify Error message for incorrect path field
    Given Open Datafusion Project to configure pipeline
    When Source is Amazon
    Then Open Amazon Properties
    Then Enter the Amazon properties for bucket "amazonPath" and incorrect path field "amazonIncorrectPathField"
    Then Verify invalid path field error
