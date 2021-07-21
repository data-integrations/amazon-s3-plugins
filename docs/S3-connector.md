# Amazon S3 Connector


Description
-----------
Use this connection to browse and sample data from S3 in Wrangler and to read data from an S3 source in a data pipeline.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection.

**Authentication Method:** Authentication method to access S3. The default value is Access Credentials.
IAM can only be used if the plugin is run in an AWS environment, such as on EMR.

**Access ID:** Amazon access ID required for authentication.

**Access Key:** Amazon access key required for authentication.

**Region:** Region to be used by the S3 Client. Note: Region is only used to sample data in Wrangler. It is not used in the S3 batch source plugin in data pipelines.