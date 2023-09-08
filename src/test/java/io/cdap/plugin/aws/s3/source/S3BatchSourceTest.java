/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.aws.s3.source;

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.aws.s3.common.S3ConnectorConfig;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * Test class for S3 Batch Source
 */
public class S3BatchSourceTest {

  @Test
  public void testValidateWithoutCredentials() throws IllegalAccessException, NoSuchFieldException {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    S3BatchSource.S3BatchConfig s3BatchConfig = new S3BatchSource.S3BatchConfig.Builder()
      .setPath("s3a://my-bucket/part1")
      .build();
    // referenceName is scoped as private in the super class, it cannot be set directly through the config builder
    Field referenceNameField = s3BatchConfig.getClass().getSuperclass().getDeclaredField("referenceName");
    referenceNameField.setAccessible(true);
    referenceNameField.set(s3BatchConfig, "ref");
    s3BatchConfig.validate(mockFailureCollector);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
    Assert.assertEquals("Connection credentials is not provided",
                        mockFailureCollector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateWithInvalidCredentials() throws IllegalAccessException, NoSuchFieldException {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    S3ConnectorConfig connection = new S3ConnectorConfig("accessID", "accessKey", "sessionToken",
                                                                "Access Credentials", "region");
    S3BatchSource.S3BatchConfig s3BatchConfig = new S3BatchSource.S3BatchConfig.Builder()
      .setPath("s3a://my-bucket/part1")
      .setConnection(connection)
      .setVerifyCredentials(Boolean.TRUE)
      .build();
    // referenceName is scoped as private in the super class, it cannot be set directly through the config builder
    Field referenceNameField = s3BatchConfig.getClass().getSuperclass().getDeclaredField("referenceName");
    referenceNameField.setAccessible(true);
    referenceNameField.set(s3BatchConfig, "ref");
    s3BatchConfig.validate(mockFailureCollector);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
    Assert.assertTrue(mockFailureCollector.getValidationFailures().get(0).getMessage()
      .contains("Invalid credentials: The security token included in the request is invalid"));
  }
  
}
