package uk.gov.justice.digital.hmpps.multiphasecleanup

import com.amazonaws.services.lambda.runtime.LambdaLogger
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient
import software.amazon.awssdk.services.redshiftdata.model.*
import uk.gov.justice.digital.hmpps.multiphasecleanup.RedshiftRepository.Companion.DB_DEFAULT_VALUE
import java.util.*

class RedshiftRepositoryTest() {

    private val redshiftDataClient = mock<RedshiftDataClient>()
    private val redshiftRepository = RedshiftRepository(redshiftDataClient, FakeEnv())
    private val logger = mock<LambdaLogger>()
    private val queryExecutionId = "executionId"
    private val executeStatementResponse = mock<ExecuteStatementResponse>()
    private val describeStatementResponse = mock<DescribeStatementResponse>()

    @Test
    fun `cleanUp executes the query to delete the expired records and returns the execution status with the number of rows deleted`() {
        val query = "DELETE FROM datamart.admin.multiphase_query_state WHERE LAST_UPDATE < SYSDATE - INTERVAL '7 days';"
        val executeStatementRequest = buildExecuteStatementRequest(query)
        val describeStatementRequest = buildDescribeStatementRequest()

        whenever(redshiftDataClient.executeStatement(ArgumentMatchers.any(ExecuteStatementRequest::class.java),),).thenReturn(executeStatementResponse)
        whenever(executeStatementResponse.id()).thenReturn(queryExecutionId)
        whenever(redshiftDataClient.describeStatement(ArgumentMatchers.any(DescribeStatementRequest::class.java))).thenReturn(describeStatementResponse)
        whenever(describeStatementResponse.status()).thenReturn(StatusString.FINISHED)
        whenever(describeStatementResponse.resultRows()).thenReturn(10L)

        val actual = redshiftRepository.cleanUp(logger)

        verify(redshiftDataClient, times(1)).executeStatement(executeStatementRequest)
        verify(redshiftDataClient, times(1)).describeStatement(describeStatementRequest)
        assertEquals(ExecutionStatus(StatusString.FINISHED,10L), actual)
    }

    private fun buildExecuteStatementRequest(query: String): ExecuteStatementRequest? =
        ExecuteStatementRequest.builder()
            .clusterIdentifier(REDSHIFT_CLUSTER_ID_VALUE)
            .database(REDSHIFT_DB_VALUE)
            .secretArn(REDSHIFT_CREDENTIAL_SECRET_ARN_VALUE)
            .sql(query)
            .build()

    private fun buildDescribeStatementRequest(): DescribeStatementRequest? =
        DescribeStatementRequest.builder()
            .id(queryExecutionId)
            .build()

}