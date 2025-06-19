package uk.gov.justice.digital.hmpps.multiphasecleanup

import uk.gov.justice.digital.hmpps.multiphasecleanup.MultiphaseCleanUpService.Companion.RETRY_DELAY_NAME
import uk.gov.justice.digital.hmpps.multiphasecleanup.MultiphaseCleanUpService.Companion.RETRY_TIMES_NAME
import uk.gov.justice.digital.hmpps.multiphasecleanup.RedshiftRepository.Companion.DB_DEFAULT_VALUE
import uk.gov.justice.digital.hmpps.multiphasecleanup.RedshiftRepository.Companion.DB_NAME
import uk.gov.justice.digital.hmpps.multiphasecleanup.RedshiftRepository.Companion.EXPIRY_TIME_NAME
import uk.gov.justice.digital.hmpps.multiphasecleanup.RedshiftRepository.Companion.REDSHIFT_CLUSTER_ID_NAME
import uk.gov.justice.digital.hmpps.multiphasecleanup.RedshiftRepository.Companion.REDSHIFT_CREDENTIAL_SECRET_ARN_NAME
import uk.gov.justice.digital.hmpps.multiphasecleanup.RedshiftRepository.Companion.REDSHIFT_STATUS_POLLING_WAIT_MS_NAME

const val REDSHIFT_CLUSTER_ID_VALUE = "REDSHIFT_CLUSTER_ID"
const val REDSHIFT_DB_VALUE = DB_DEFAULT_VALUE
const val REDSHIFT_CREDENTIAL_SECRET_ARN_VALUE = "REDSHIFT_CREDENTIAL_SECRET_ARN"
const val EXPIRY_TIME_VALUE = "7 days"

class FakeEnv(
    private val properties: Map<String, String> = mapOf(
        RETRY_TIMES_NAME to "2",
        RETRY_DELAY_NAME to "0",
        REDSHIFT_STATUS_POLLING_WAIT_MS_NAME to "0",
        REDSHIFT_CLUSTER_ID_NAME to REDSHIFT_CLUSTER_ID_VALUE,
        DB_NAME to REDSHIFT_DB_VALUE,
        REDSHIFT_CREDENTIAL_SECRET_ARN_NAME to REDSHIFT_CREDENTIAL_SECRET_ARN_VALUE,
        EXPIRY_TIME_NAME to EXPIRY_TIME_VALUE,
    )
): Env() {

    override fun get(name: String): String? {
        return properties[name]
    }
}