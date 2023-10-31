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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import javax.annotation.Nullable;

/**
 * S3 connector config which contains the credential related information
 */
public class S3ConnectorConfig extends PluginConfig {
  public static final String ACCESS_CREDENTIALS = "Access Credentials";
  public static final String NAME_ACCESS_ID = "accessID";
  public static final String NAME_ACCESS_KEY = "accessKey";
  public static final String NAME_SESSION_TOKEN = "sessionToken";
  public static final String NAME_AUTH_METHOD = "authenticationMethod";
  public static final String NAME_REGION = "region";

  @Macro
  @Nullable
  @Description("Access ID of the Amazon S3 instance to connect to.")
  private String accessID;

  @Macro
  @Nullable
  @Description("Access Key of the Amazon S3 instance to connect to.")
  private String accessKey;

  @Macro
  @Nullable
  @Description("Session Token of the Amazon S3 instance to connect to, if this is a temporary credential")
  private String sessionToken;

  @Macro
  @Nullable
  @Description("Authentication method to access S3. Defaults to Access Credentials.")
  private String authenticationMethod;

  @Macro
  @Nullable
  @Description("Region to be used by the S3 Client.")
  private String region;

  public S3ConnectorConfig() {
    authenticationMethod = ACCESS_CREDENTIALS;
  }

  public S3ConnectorConfig(
      @Nullable String accessID,
      @Nullable String accessKey,
      @Nullable String sessionToken,
      @Nullable String authenticationMethod,
      @Nullable String region) {
    this.accessID = accessID;
    this.accessKey = accessKey;
    this.sessionToken = sessionToken;
    this.authenticationMethod = authenticationMethod;
    this.region = region;
  }

  @Nullable
  public String getAccessID() {
    return accessID;
  }

  @Nullable
  public String getAccessKey() {
    return accessKey;
  }

  @Nullable
  public String getSessionToken() {
    return sessionToken;
  }

  @Nullable
  public String getAuthenticationMethod() {
    return authenticationMethod;
  }

  @Nullable
  public String getRegion() {
    return region;
  }

  public boolean isAccessCredentials() {
    return ACCESS_CREDENTIALS.equalsIgnoreCase(authenticationMethod);
  }

  public void validate(FailureCollector collector, Boolean verifyCredentials) {
    if (containsMacro(NAME_AUTH_METHOD)) {
      return;
    }

    if (!ACCESS_CREDENTIALS.equals(authenticationMethod)) {
      return;
    }

    if (!containsMacro("accessID") && (accessID == null || accessID.isEmpty())) {
      collector.addFailure("The Access ID must be specified if authentication method is Access Credentials.", null)
        .withConfigProperty(NAME_ACCESS_ID).withConfigProperty(NAME_AUTH_METHOD);
      return;
    }
    if (!containsMacro("accessKey") && (accessKey == null || accessKey.isEmpty())) {
      collector.addFailure("The Access Key must be specified if authentication method is Access Credentials.", null)
        .withConfigProperty(NAME_ACCESS_KEY).withConfigProperty(NAME_AUTH_METHOD);
      return;
    }
    if (verifyCredentials && !containsMacro(NAME_ACCESS_ID) && !containsMacro(NAME_ACCESS_KEY) &&
      !containsMacro(NAME_SESSION_TOKEN)) {
      validateCredentials(collector);
    }
  }

  private void validateCredentials(FailureCollector collector) {
    AWSSecurityTokenService stsClient = getSTSClient(Regions.DEFAULT_REGION.getName());
    try {
      stsClient.getCallerIdentity(new GetCallerIdentityRequest());
    } catch (AWSSecurityTokenServiceException exception) {
      collector.addFailure(String.format("Invalid credentials: %s.", exception.getMessage()),
                           "Please provide valid credentials.");
    }
  }

  private AWSSecurityTokenService getSTSClient(String region) {
    AWSSecurityTokenServiceClientBuilder builder = AWSSecurityTokenServiceClientBuilder.standard();
    if (!Strings.isNullOrEmpty(region)) {
      builder.setRegion(region);
    }
    AWSCredentials creds;
    if (getSessionToken() == null) {
      creds = new BasicAWSCredentials(getAccessID(), getAccessKey());
    } else {
      creds = new BasicSessionCredentials(getAccessID(), getAccessKey(), getSessionToken());
    }
    return builder.withCredentials(new AWSStaticCredentialsProvider(creds))
      .build();
  }
}
