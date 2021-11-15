# Amazon S3 Batch Sink


Description
-----------
This sink is used whenever you need to write to Amazon S3 in various formats. For example,
you might want to create daily snapshots of a database by reading the entire contents of a
table, writing to this sink, and then other programs can analyze the contents of the
specified file.


Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Use Connection** Whether to use a connection, if a connection is used,
the credentials does not need to be provided.

**Connection** Name of the connection to use, should have the macro function ${conn(connection-name)} to provide.

**Path:** Path to write to. For example, s3a://<bucket>/path/to/output

**Path Suffix:** Time format for the output directory that will be appended to the path.
For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'.
If not specified, nothing will be appended to the path."

**Format:** Format to write the records in.
The format must be one of 'json', 'avro', 'parquet', 'csv', 'tsv', or 'delimited'.

**Delimiter:** Delimiter to use if the format is 'delimited'.
The delimiter will be ignored if the format is anything other than 'delimited'.

**Authentication Method:** Authentication method to access S3. The default value is Access Credentials.
IAM can only be used if the plugin is run in an AWS environment, such as on EMR.

**Access ID:** Amazon access ID required for authentication.

**Access Key:** Amazon access key required for authentication.

**Session Token:** Amazon session token required for authentication. Only required for temporary credentials.
Temporary credentials are only supported for S3A paths.

**Enable Encryption:** Whether to enable server side encryption. The sole supported algorithm is AES256.

**File System Properties:** Additional properties to use with the OutputFormat when reading the data.
