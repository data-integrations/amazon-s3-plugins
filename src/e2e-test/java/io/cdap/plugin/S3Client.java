package io.cdap.plugin;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.aws.s3.common.S3ConnectorConfig;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

/**
 *  S3 client for e2e test.
 */
public class S3Client {
  private static AmazonS3 s3Client = null;

  public S3Client() {
  }

  private static S3ConnectorConfig initializeConfig() {
    return new S3ConnectorConfig(PluginPropertyUtils.pluginProp("accessID"),
                                 PluginPropertyUtils.pluginProp("accessKey"), null, "Access Credentials",
                                 PluginPropertyUtils.pluginProp("region"));
  }

  private static AmazonS3 getS3Client() throws IOException {
     if (s3Client == null) {
       AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
       S3ConnectorConfig config = initializeConfig();
       assert config.getRegion() != null;
       builder.setRegion(config.getRegion());
       AWSCredentials creds;
       assert config.getAccessID() != null;
       assert config.getAccessKey() != null;
       creds = new BasicAWSCredentials(config.getAccessID(), config.getAccessKey());
       s3Client = builder.withCredentials(new AWSStaticCredentialsProvider(creds)).build();
     }
     return s3Client;
  }

  public static String createBucket(String bucketName) throws IOException {
    return getS3Client().createBucket(bucketName).getName();
  }

  public static void deleteBucket(String bucketName) throws IOException {
    AmazonS3 s3Client = getS3Client();
    for (S3ObjectSummary s3ObjectSummary: s3Client.listObjects(bucketName).getObjectSummaries()) {
      s3Client.deleteObject(bucketName, s3ObjectSummary.getKey());
    }
    s3Client.deleteBucket(bucketName);
  }


  public static void uploadObject(String bucketName, String filePath) throws URISyntaxException, IOException {
    File file = new File(
      String.valueOf(Paths.get(Objects.requireNonNull(S3Client.class.getResource("/" + filePath)).toURI()))
    );
    AmazonS3 s3Client = getS3Client();
    s3Client.putObject(bucketName, filePath, file);
  }

  public static List<S3ObjectSummary> listObjects(String s3Bucket) throws IOException {
    return getS3Client().listObjects(s3Bucket).getObjectSummaries();
  }
}
