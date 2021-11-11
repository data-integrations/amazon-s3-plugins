/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.plugin.aws.s3.common;

/**
 * Constant for S3
 */
public class S3Constants {
  public static final String S3A_ACCESS_KEY = "fs.s3a.access.key";
  public static final String S3A_SECRET_KEY = "fs.s3a.secret.key";
  public static final String S3A_SESSION_TOKEN = "fs.s3a.session.token";
  public static final String S3A_CREDENTIAL_PROVIDERS = "fs.s3a.aws.credentials.provider";
  public static final String S3A_TEMP_CREDENTIAL_PROVIDERS =
      "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";
  public static final String S3A_SIMPLE_CREDENTIAL_PROVIDERS =
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
  public static final String S3A_ENCRYPTION = "fs.s3a.server-side-encryption-algorithm";
  public static final String S3N_ACCESS_KEY = "fs.s3n.awsAccessKeyId";
  public static final String S3N_SECRET_KEY = "fs.s3n.awsSecretAccessKey";
  public static final String S3N_ENCRYPTION = "fs.s3n.server-side-encryption-algorithm";
}
