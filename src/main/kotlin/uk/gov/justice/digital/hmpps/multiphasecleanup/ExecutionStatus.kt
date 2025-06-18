package uk.gov.justice.digital.hmpps.multiphasecleanup

import software.amazon.awssdk.services.redshiftdata.model.StatusString

data class ExecutionStatus(
    val status: StatusString,
    val rowsDeleted: Long,
) {
}