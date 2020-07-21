package fi.hsl.transitdata.rata_digitraffic.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory.getLogger
import kotlin.properties.ReadOnlyProperty
import kotlin.reflect.KProperty
import kotlin.reflect.full.companionObject

class LoggerDelegate<in R : Any> : ReadOnlyProperty<R, Logger> {
    private lateinit var logger: Logger

    override fun getValue(thisRef: R, property: KProperty<*>): Logger {
        if (!::logger.isInitialized) {
            logger = getLogger(getClassForLogging(thisRef.javaClass))
        }

        return logger
    }

    private fun <T : Any> getClassForLogging(javaClass: Class<T>): Class<*> {
        return javaClass.enclosingClass?.takeIf {
            it.kotlin.companionObject?.java == javaClass
        } ?: javaClass
    }
}