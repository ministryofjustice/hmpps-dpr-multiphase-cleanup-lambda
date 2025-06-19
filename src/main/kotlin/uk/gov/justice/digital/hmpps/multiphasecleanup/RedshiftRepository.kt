package uk.gov.justice.digital.hmpps.multiphasecleanup

import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.logging.LogLevel
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient
import software.amazon.awssdk.services.redshiftdata.model.DescribeStatementRequest
import software.amazon.awssdk.services.redshiftdata.model.DescribeStatementResponse
import software.amazon.awssdk.services.redshiftdata.model.ExecuteStatementRequest
import software.amazon.awssdk.services.redshiftdata.model.StatusString

class RedshiftRepository(
    private val redshiftClient: RedshiftDataClient,
    private val env: Env
) {
    companion object {
        const val REDSHIFT_CLUSTER_ID_NAME = "REDSHIFT_CLUSTER_ID"
        const val REDSHIFT_CREDENTIAL_SECRET_ARN_NAME = "REDSHIFT_CREDENTIAL_SECRET_ARN"
        const val REDSHIFT_STATUS_POLLING_WAIT_MS_NAME = "REDSHIFT_STATUS_POLLING_WAIT_MS"
        const val REDSHIFT_STATUS_POLLING_WAIT_MS_VALUE = 500L
        const val DB_NAME = "DB_NAME"
        const val DB_DEFAULT_VALUE = "datamart"
        const val MULTIPHASE_SCHEMA_NAME = "MULTIPHASE_SCHEMA"
        const val MULTIPHASE_SCHEMA_DEFAULT_VALUE = "admin"
        const val MULTIPHASE_TABLE_NAME = "MULTIPHASE_TABLE"
        const val MULTIPHASE_TABLE_DEFAULT_VALUE = "multiphase_query_state"
        const val EXPIRY_TIME_NAME = "EXPIRY_TIME"
        const val EXPIRY_TIME_DEFAULT_VALUE = "7 days"
    }

    fun cleanUp(logger: LambdaLogger): ExecutionStatus {
        return executeQueryAndWaitForCompletion("DELETE FROM ${getFullTableName()} WHERE LAST_UPDATE < SYSDATE - INTERVAL '${getExpiryTime()}';", logger)
    }

    private fun getFullTableName() =
        "${(env.get(DB_NAME) ?: DB_DEFAULT_VALUE)}.${(env.get(MULTIPHASE_SCHEMA_NAME) ?: MULTIPHASE_SCHEMA_DEFAULT_VALUE)}.${(env.get(MULTIPHASE_TABLE_NAME) ?: MULTIPHASE_TABLE_DEFAULT_VALUE)}"

    private fun getExpiryTime() = env.get(EXPIRY_TIME_NAME) ?: EXPIRY_TIME_DEFAULT_VALUE

    private fun executeQueryAndWaitForCompletion(query:String, logger: LambdaLogger): ExecutionStatus {

        logger.log("Executing cleanup query: $query", LogLevel.DEBUG)
        val executionId = redshiftClient.executeStatement(
            buildExecuteStatementRequest(query)
        ).id()
        logger.log("Executed cleanup statement and got ID: $executionId", LogLevel.DEBUG)
        val describeStatementRequest = DescribeStatementRequest.builder()
            .id(executionId)
            .build()
        var describeStatementResponse: DescribeStatementResponse
        do {
            Thread.sleep(env.get(REDSHIFT_STATUS_POLLING_WAIT_MS_NAME)?.toLong() ?: REDSHIFT_STATUS_POLLING_WAIT_MS_VALUE)
            describeStatementResponse = redshiftClient.describeStatement(describeStatementRequest)
            if (describeStatementResponse.status() == StatusString.FAILED) {
                logger.log("Statement with execution ID: $executionId failed with the following error: ${describeStatementResponse.error()}",
                    LogLevel.ERROR)
                return ExecutionStatus(describeStatementResponse.status(), describeStatementResponse.resultRows())
            } else if (describeStatementResponse.status() == StatusString.ABORTED) {
                logger.log("Statement with execution ID: $executionId was aborted", LogLevel.ERROR)
                return ExecutionStatus(describeStatementResponse.status(), describeStatementResponse.resultRows())
            }
        }
        while (describeStatementResponse.status() != StatusString.FINISHED)
        logger.log("Finished executing statement with id: $executionId, status: ${describeStatementResponse.statusAsString()}, result rows: ${describeStatementResponse.resultRows()}", LogLevel.DEBUG)
        return ExecutionStatus(describeStatementResponse.status(), describeStatementResponse.resultRows())
    }

    private fun buildExecuteStatementRequest(query: String): ExecuteStatementRequest? =
        ExecuteStatementRequest.builder()
            .clusterIdentifier(env.get(REDSHIFT_CLUSTER_ID_NAME))
            .database(env.get(DB_NAME)?: DB_DEFAULT_VALUE)
            .secretArn(env.get(REDSHIFT_CREDENTIAL_SECRET_ARN_NAME))
            .sql(query)
            .build()
}