# Amazon S3 Connection


Description
-----------
Use this connection to access data in Amazon S3.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection.

**Authentication Method:** Authentication method to access S3. The default value is Access Credentials.
IAM can only be used if the plugin is run in an AWS environment, such as on EMR.

**Access ID:** Amazon access ID required for authentication.

**Access Key:** Amazon access key required for authentication.

**Session Token:** Amazon session token required for authentication. Only required for temporary credentials.
Temporary credentials are only supported for S3A paths.

**Region:** Region to be used by the S3 Client. Note: Region is only used to sample data in Wrangler. It is not used in the S3 batch source plugin in data pipelines.

Path of the connection
----------------------
To browse, get a sample from, or get the specification for this connection through
[Pipeline Microservices](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/975929350/Pipeline+Microservices), the `path`
property is required in the request body. It's an absolute Amazon S3 path of a file or folder.
