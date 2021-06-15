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

import org.junit.Assert;
import org.junit.Test;

import java.util.stream.IntStream;

/**
 * S3 path test
 */
public class S3PathTest {
  @Test
  public void testGetPath() {
    S3Path s3Path = S3Path.from("s3n://my-bucket/part1");
    Assert.assertEquals("s3n://my-bucket/part1", s3Path.getFullPath());
    s3Path = S3Path.from("my-bucket/part1");
    Assert.assertEquals("s3n://my-bucket/part1", s3Path.getFullPath());
    s3Path = S3Path.from("/my-bucket/part1");
    Assert.assertEquals("s3n://my-bucket/part1", s3Path.getFullPath());

    s3Path = S3Path.from("s3n://my-bucket/part1/part2");
    Assert.assertEquals("s3n://my-bucket/part1/part2", s3Path.getFullPath());
    s3Path = S3Path.from("my-bucket/part1/part2");
    Assert.assertEquals("s3n://my-bucket/part1/part2", s3Path.getFullPath());
    s3Path = S3Path.from("/my-bucket/part1/part2");
    Assert.assertEquals("s3n://my-bucket/part1/part2", s3Path.getFullPath());
    s3Path = S3Path.from("s3n://my-bucket/part1/hello world");
    Assert.assertEquals("s3n://my-bucket/part1/hello world", s3Path.getFullPath());
    s3Path = S3Path.from("s3n://my-bucket/hello world 1/hello world 2");
    Assert.assertEquals("s3n://my-bucket/hello world 1/hello world 2", s3Path.getFullPath());

    assertFailure(() -> S3Path.from(""));
    assertFailure(() -> S3Path.from("s3n:/abc/"));
    assertFailure(() -> S3Path.from("s3n:///abc/"));
    assertFailure(() -> S3Path.from("s3n://test space in bucket name/"));
    assertFailure(() -> S3Path.from("file://abc/"));
  }

  @Test
  public void testGetBucket() {
    Assert.assertEquals("my-bucket", S3Path.from("s3n://my-bucket/part1 test").getBucket());
    Assert.assertEquals("my-bucket", S3Path.from("my-bucket/part1").getBucket());
    Assert.assertEquals("my-bucket", S3Path.from("/my-bucket/part1").getBucket());

    Assert.assertEquals("my-bucket", S3Path.from("s3n://my-bucket/part1/part2").getBucket());
    Assert.assertEquals("my-bucket", S3Path.from("my-bucket/part1/part2").getBucket());
    Assert.assertEquals("my-bucket", S3Path.from("/my-bucket/part1/part2").getBucket());

    assertFailure(() -> S3Path.from(""));

    // bucket length should be 3-63
    assertFailure(() -> S3Path.from("s3n://sh"));

    StringBuilder sb = new StringBuilder();
    IntStream.range(0, 100).forEach(i -> sb.append("a"));
    assertFailure(() -> S3Path.from("s3n://" + sb.toString()));
  }

  @Test
  public void testGetObject() {
    Assert.assertEquals("csvExample.csv", S3Path.from("s3n://my-bucket/csvExample.csv").getName());
    Assert.assertEquals("textExample.txt", S3Path.from("my-bucket/textExample.txt").getName());
    Assert.assertEquals("jsonExample.json", S3Path.from("/my-bucket/jsonExample.json").getName());
    Assert.assertEquals("avroExample.avro", S3Path.from("s3n://my-bucket/avroExample.avro").getName());
    Assert.assertEquals("parquetExample.parquet",
                        S3Path.from("my-bucket/parquetExample.parquet").getName());
    Assert.assertEquals("blobExample.blb", S3Path.from("/my-bucket/blobExample.blb").getName());

    assertFailure(() -> S3Path.from(""));
  }

  @Test
  public void testSlashes() {
    for (String path : new String[] { "s3n://ba0/n0", "ba0/n0", "/ba0/n0" }) {
      S3Path s3Path = S3Path.from(path);
      Assert.assertEquals("ba0", s3Path.getBucket());
      Assert.assertEquals("n0", s3Path.getName());
    }

    for (String path : new String[] { "s3n://ba0/", "s3n://ba0", "/ba0", "/ba0/" }) {
      S3Path s3Path = S3Path.from(path);
      Assert.assertEquals("ba0", s3Path.getBucket());
      Assert.assertTrue(s3Path.getName().isEmpty());
    }
  }

  private void assertFailure(Runnable runnable) {
    try {
      runnable.run();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
