/*
 * Copyright © 2015 Cask Data, Inc.
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
 */

package co.cask.hydrator.plugin.sink;

/**
 * Class to define property names for source and sinks
 */
public final class Properties {

  /**
   * Class to hold properties for S3BatchSource
   */
  public static class S3 {
    public static final String ACCESS_ID = "accessID";
    public static final String ACCESS_KEY = "accessKey";
    public static final String PATH = "path";
    public static final String FILE_REGEX = "fileRegex";
    public static final String TIME_TABLE = "timeTable";
    public static final String INPUT_FORMAT_CLASS = "inputFormatClass";
    public static final String MAX_SPLIT_SIZE = "maxSplitSize";
    public static final String IGNORE_NON_EXISTING_FOLDERS = "ignoreNonExistingFolders";
    public static final String RECURSIVE = "recursive";
  }

  /**
   * Configuration for S3BatchSink
   */
  public static class S3BatchSink {
    public static final String BASE_PATH = "basePath";
    public static final String SCHEMA = "schema";
    public static final String PATH_FORMAT = "pathFormat";
    public static final String AUTHENTICATION_METHOD = "authenticationMethod";
    public static final String ENABLE_ENCRYPTION = "enableEncryption";
  }

  private Properties() {
  }
}
