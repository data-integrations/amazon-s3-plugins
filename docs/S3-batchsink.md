# Amazon S3 Batch Sink


Description
-----------
A batch sink for writing to Amazon S3 in various formats.


Use Case
--------
This sink is used whenever you need to write to Amazon S3 in Avro format. For example,
you might want to create daily snapshots of a database by reading the entire contents of a
table, writing to this sink, and then other programs can analyze the contents of the
specified file. The output of the run will be stored in a directory with suffix
'yyyy-MM-dd-HH-mm' from the base path provided.


Properties
----------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**authenticationMethod:** Authentication method to access S3. Defaults to Access Credentials.
 User need to have AWS environment only to use IAM role based authentication. 
  URI scheme should be s3a:// for S3AFileSystem or s3n:// for S3NativeFileSystem. (Macro-enabled)

**accessID:** Access ID of the Amazon S3 instance to connect to. (Macro-enabled)

**accessKey:** Access Key of the Amazon S3 instance to connect to. (Macro-enabled)

**basePath:** The S3 path where the data is stored. Example: 's3a://logs'. (Macro-enabled)

**enableEncryption:** Server side encryption. Defaults to True. Sole supported algorithm is AES256. (Macro-enabled)

**fileSystemProperties:** A JSON string representing a map of properties needed for the
distributed file system. The property names needed for S3 (*accessID* and *accessKey*)
will be included as ``'fs.s3a.access.key'`` and ``'fs.s3a.secret.key'`` for S3AFileSystem. 
For S3NativeFileSystem ``'fs.s3n.awsSecretAccessKey'`` and ``'fs.s3n.awsAccessKeyId'`` will be used. (Macro-enabled)

**Directory Time Format:** The format for the path that will be suffixed to the basePath; for
example: the format ``'yyyy-MM-dd-HH-mm'`` will create a file path ending in
``'2015-01-01-20-42'``. Default format used is ``'yyyy-MM-dd-HH-mm'``. (Macro-enabled)

**Format:** Format to write the records in.
The format must be one of 'json', 'avro', 'parquet', 'csv', 'tsv', or 'delimited'. (Macro-enabled)

**schema:** The Schema of the record being written to the sink as a JSON object. (Macro-enabled)


Example
-------
This example will use Access Credentials authentication and write to an S3 output located at ``s3a://logs``. It will write data in
Avro format compressed using Snappy format and using the given schema. Every time the pipeline 
runs, a new output directory from the base path (``s3a://logs``) will be created which 
will have the directory name corresponding to the start time in ``yyyy-MM-dd-HH-mm`` format:

    {
        "name": "S3Avro",
        "type": "batchsink",
        "properties": {
            "authenticationMethod": "Access Credentials",
            "accessKey": "key",
            "accessID": "ID",
            "basePath": "s3a://logs",
            "pathFormat": "yyyy-MM-dd-HH-mm",
            "compressionCodec": "Snappy",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"user\",
                \"fields\":[
                    {\"name\":\"id\",\"type\":\"long\"},
                    {\"name\":\"name\",\"type\":\"string\"},
                    {\"name\":\"birthyear\",\"type\":\"int\"}
                ]
            }"
        }
    }