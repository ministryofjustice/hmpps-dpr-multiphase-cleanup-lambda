package uk.gov.justice.digital.hmpps.multiphasecleanup


open class Env {
    open fun get(name: String): String? = System.getenv(name)
}