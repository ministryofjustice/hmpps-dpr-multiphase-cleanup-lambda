package uk.gov.justice.digital.hmpps.multiphasecleanup

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.LambdaLogger
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito.mock
import org.mockito.kotlin.*
import software.amazon.awssdk.services.redshiftdata.model.StatusString

class MultiphaseCleanUpServiceTest {

    private val context: Context = mock()
    private val logger: LambdaLogger = mock()
    private val redshiftRepository = mock<RedshiftRepository>()
    private val multiphaseCleanUpService: MultiphaseCleanUpService = MultiphaseCleanUpService(FakeEnv(), redshiftRepository)

    @BeforeEach
    fun setUp() {
        whenever(context.logger).thenReturn(logger)
    }

    @Test
    fun `handleRequest returns the number of rows deleted`() {
        val executionStatus = ExecutionStatus(StatusString.FINISHED, 1)
        whenever(redshiftRepository.cleanUp(any())).thenReturn(executionStatus)

        val actual = multiphaseCleanUpService.handleRequest(mutableMapOf(), context)

        verify(redshiftRepository, times(1)).cleanUp(logger)
        assertEquals("Completed execution and deleted ${executionStatus.rowsDeleted} rows in total.", actual)
    }

    @Test
    fun `handleRequest returns context was null if the Lambda Context was null`() {
        val actual = multiphaseCleanUpService.handleRequest(mutableMapOf(), null)
        assertEquals("Context was null.", actual)
    }

    @ParameterizedTest
    @ValueSource(strings = ["FAILED", "ABORTED"])
    fun `cleanUp should be retried up to 2 times if the query does not succeed`(status: String) {
        val exception = assertThrows<CleanUpFailedException> {
            val executionStatus = ExecutionStatus(StatusString.fromValue(status), 0)

            whenever(redshiftRepository.cleanUp(any())).thenReturn(executionStatus)

            multiphaseCleanUpService.handleRequest(mutableMapOf(), context)

            verify(redshiftRepository, times(3)).cleanUp(logger)
        }
        assertEquals( "Failed to delete expired records after 3 attempts.", exception.message)
    }

    @Test
    fun `cleanUp should stop retrying when the query succeeds`() {
        val failedExecutionStatus = ExecutionStatus(StatusString.FAILED, 0)
        val finishedExecutionStatus = ExecutionStatus(StatusString.FINISHED, 5)

        whenever(redshiftRepository.cleanUp(any()))
            .thenReturn(failedExecutionStatus)
            .thenReturn(finishedExecutionStatus)

        val actual = multiphaseCleanUpService.handleRequest(mutableMapOf(), context)

        verify(redshiftRepository, times(2)).cleanUp(logger)
        assertEquals("Completed execution and deleted ${finishedExecutionStatus.rowsDeleted} rows in total.", actual)
    }

}