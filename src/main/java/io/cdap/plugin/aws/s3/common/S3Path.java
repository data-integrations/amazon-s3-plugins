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

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A path on S3. Contains information about the bucket and file name (if applicable).
 * A path is of the form s3n://bucket/name.
 */
public class S3Path {
  public static final String ROOT_DIR = "/";
  public static final String SCHEME = "s3n://";
  private final String fullPath;
  private final String bucket;
  private final String name;

  private S3Path(String fullPath, String bucket, String name) {
    this.fullPath = fullPath;
    this.bucket = bucket;
    this.name = name;
  }

  public String getFullPath() {
    return fullPath;
  }

  public String getBucket() {
    return bucket;
  }

  /**
   * @return the object name. This will be an empty string if the path represents a bucket.
   */
  public String getName() {
    return name;
  }

  boolean isBucket() {
    return name.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    S3Path s3Path = (S3Path) o;
    return Objects.equals(fullPath, s3Path.fullPath) &&
             Objects.equals(bucket, s3Path.bucket) &&
             Objects.equals(name, s3Path.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fullPath, bucket, name);
  }

  /**
   * Parse the given path string into a S3Path. Paths are expected to be of the form
   * s3n://bucket/dir0/dir1/file, or bucket/dir0/dir1/file.
   *
   * @param path the path string to parse
   * @return the S3Path for the given string.
   * @throws IllegalArgumentException if the path string is invalid
   */
  public static S3Path from(String path) {
    if (path.isEmpty()) {
      throw new IllegalArgumentException("S3 path can not be empty. The path must be of form " +
                                           "'s3n://<bucket-name>/path'.");
    }

    if (path.startsWith(ROOT_DIR)) {
      path = path.substring(1);
    } else if (path.startsWith(SCHEME)) {
      path = path.substring(SCHEME.length());
    }

    String bucket = path;
    int idx = path.indexOf(ROOT_DIR);
    // if the path within bucket is provided, then only get the bucket
    if (idx > 0) {
      bucket = path.substring(0, idx);
    }

    if (bucket.length() < 3 || bucket.length() > 63) {
      throw new IllegalArgumentException("Invalid bucket name, the bucket name length must be between 3 characters " +
                                           "and 63 characters.");
    }

    if (!Pattern.matches("[a-z0-9-.]+", bucket)) {
      throw new IllegalArgumentException(
        String.format("Invalid bucket name in path '%s'. Bucket name should only contain lower case alphanumeric, " +
                        "'-' and '.'. Please follow S3 bucket naming convention: " +
                        "https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html", path));
    }

    String file = idx > 0 ? path.substring(idx).replaceAll("^/", "") : "";
    return new S3Path(SCHEME + bucket + "/" + file, bucket, file);
  }
}
