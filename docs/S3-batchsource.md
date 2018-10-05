# Amazon S3 Batch Source


Description
-----------
Batch source to use Amazon S3 as a Source.


Use Case
--------
This source is used whenever you need to read from Amazon S3.
For example, you may want to read in log files from S3 every hour and then store
the logs in a TimePartitionedFileSet.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**authenticationMethod:** Authentication method to access S3. Defaults to Access Credentials.
 User need to have AWS environment only to use IAM role based authentication. URI scheme should be s3a:// for S3AFileSystem or s3n:// for S3NativeFileSystem. (Macro-enabled)

**accessID:** Access ID of the Amazon S3 instance to connect to. Mandatory if authentication method is Access credentials. (Macro-enabled)

**accessKey:** Access Key of the Amazon S3 instance to connect to. Mandatory if authentication method is Access credentials. (Macro-enabled)

**path:** Path to file(s) to be read. If a directory is specified,
terminate the path name with a '/'. The path uses filename expansion (globbing) to read files. (Macro-enabled)

**Format:** Format of the data to read.
The format must be one of 'avro', 'blob', 'csv', 'delimited', 'json', 'parquet', 'text', or 'tsv'.
If the format is 'blob', every input file will be read into a separate record.
The 'blob' format also requires a schema that contains a field named 'body' of type 'bytes'.
If the format is 'text', the schema must contain a field named 'body' of type 'string'. (Macro-enabled)

**fileRegex:** Regex to filter out files in the path. It accepts regular expression which is applied to the complete
path and returns the list of files that match the specified pattern.

**maxSplitSize:** Maximum split-size for each mapper in the MapReduce Job. Defaults to 128MB. (Macro-enabled)

**ignoreNonExistingFolders:** Identify if path needs to be ignored or not, for case when directory or file does not
exists. If set to true it will treat the not present folder as 0 input and log a warning. Default is false.

**recursive:** Boolean value to determine if files are to be read recursively from the path. Default is false.


Example
-------
This example connects to Amazon S3 using Access Credentials and reads in files found in the specified directory while
using the stateful ``timefilter``, which ensures that each file is read only once. The ``timefilter``
requires that files be named with either the convention "yy-MM-dd-HH..." (S3) or "...'.'yy-MM-dd-HH..."
(Cloudfront). The stateful metadata is stored in a table named 'timeTable'. With the maxSplitSize
set to 1MB, if the total size of the files being read is larger than 1MB, CDAP will
configure Hadoop to use one mapper per MB:

    {
        "name": "S3",
        "type": "batchsource",
        "properties": {
            "authenticationMethod": "Access Credentials",
            "accessKey": "key",
            "accessID": "ID",
            "path": "s3a://path/to/logs/",
            "fileRegex": "timefilter",
            "timeTable": "timeTable",
            "maxSplitSize": "1048576",
            "ignoreNonExistingFolders": "false",
            "recursive": "false"
        }
    }
