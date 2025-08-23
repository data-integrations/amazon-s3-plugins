/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.plugin.aws.s3.common;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ErrorUtils.ActionErrorPair;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.etl.api.exception.ErrorContext;
import io.cdap.cdap.etl.api.exception.ErrorDetailsProvider;
import java.util.List;

/**
 * Error details provided for the Amazon S3
 **/
public final class AmazonErrorDetailsProvider implements ErrorDetailsProvider {

  static final String S3_EXTERNAL_DOC =
      "https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html";

  @Override
  public ProgramFailureException getExceptionDetails(Exception e, ErrorContext errorContext) {
    List<Throwable> causalChain = Throwables.getCausalChain(e);
    for (Throwable t : causalChain) {
      if (t instanceof ProgramFailureException) {
        // if causal chain already has program failure exception, return null to avoid double wrap.
        return null;
      }
      if (t instanceof AmazonS3Exception) {
        AmazonS3Exception amazonServiceException = (AmazonS3Exception) t;
        int statusCode = amazonServiceException.getStatusCode();
        ActionErrorPair pair = ErrorUtils.getActionErrorByStatusCode(statusCode);
        String errorReason = String.format("%s %s. %s. For more details, see %s", statusCode,
            amazonServiceException.getErrorMessage(), pair.getCorrectiveAction(), S3_EXTERNAL_DOC);
        return ErrorUtils.getProgramFailureException(new ErrorCategory(
            ErrorCategory.ErrorCategoryEnum.PLUGIN, amazonServiceException.getErrorCode()),
            errorReason, amazonServiceException.getMessage(), ErrorType.USER, true,
            ErrorCodeType.HTTP, String.valueOf(statusCode), S3_EXTERNAL_DOC, t);
      }
    }
    return null;
  }
}
