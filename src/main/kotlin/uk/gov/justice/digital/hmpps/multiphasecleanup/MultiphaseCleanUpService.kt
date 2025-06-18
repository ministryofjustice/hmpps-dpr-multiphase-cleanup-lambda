package uk.gov.justice.digital.hmpps.multiphasecleanup

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.logging.LogLevel
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient
import software.amazon.awssdk.services.redshiftdata.model.StatusString

class MultiphaseCleanUpService(
    private val env: Env = Env(),
    private val redshiftRepository: RedshiftRepository = RedshiftRepository(
            RedshiftDataClient.builder()
                .region(Region.EU_WEST_2)
                .build(), env),
) : RequestHandler<MutableMap<String, Any>, String> {

    companion object {
        const val RETRY_TIMES_NAME = "RETRY_TIMES"
        const val RETRY_TIMES_VALUE = 2
        const val RETRY_DELAY_NAME = "RETRY_DELAY"
        const val RETRY_DELAY_VALUE = 500L
    }

    override fun handleRequest(payload: MutableMap<String, Any>, context: Context?): String {
        if (context != null) {
            val logger = context.logger
            logger.log("Multiphase cleanup lambda invoked.", LogLevel.INFO)
            val rowsDeleted = retry(logger) { redshiftRepository.cleanUp(logger) }
            return "Completed execution and deleted $rowsDeleted rows in total."
        }
        return "Context was null."
    }

    private fun retry(
        logger: LambdaLogger,
        times: Int = env.get(RETRY_TIMES_NAME)?.toInt() ?: RETRY_TIMES_VALUE,
        delayInMillis: Long = env.get(RETRY_DELAY_NAME)?.toLong()?: RETRY_DELAY_VALUE,
        updateFun: () -> ExecutionStatus): Long {
        var attempt = 0
        var executionStatus = updateFun()
        while (executionStatus.status != StatusString.FINISHED && attempt < times ) {
            logger.log("Failed to delete expired records. Retrying ${attempt + 1}", LogLevel.WARN)
            attempt++
            Thread.sleep(delayInMillis)
            executionStatus = updateFun()
        }
        if (executionStatus.status != StatusString.FINISHED) {
            throw CleanUpFailedException("Failed to delete expired records after ${RETRY_TIMES_VALUE + 1} attempts.")
        }
        return executionStatus.rowsDeleted
    }


}