package fi.hsl.transitdata.rata_digitraffic.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.io.InputStream

object JsonHelper {
    val objectMapper: ObjectMapper = ObjectMapper().registerModule(KotlinModule()).registerModule(JavaTimeModule())

    inline fun <reified T> parse(byteArray: ByteArray): T {
        return objectMapper.readValue(byteArray, T::class.java)
    }

    inline fun <reified T> parse(inputStream: InputStream): T {
        return objectMapper.readValue(inputStream, T::class.java)
    }

    inline fun <reified T> parseList(byteArray: ByteArray): List<T> {
        return objectMapper.readerForListOf(T::class.java).readValue(byteArray) as List<T>
    }

    inline fun <reified T> parseList(inputStream: InputStream): List<T> {
        return objectMapper.readerForListOf(T::class.java).readValue(inputStream) as List<T>
    }
}