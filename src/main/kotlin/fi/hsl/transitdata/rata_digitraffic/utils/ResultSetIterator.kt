package fi.hsl.transitdata.rata_digitraffic.utils

import java.sql.ResultSet

inline fun <T> ResultSet.iterator(crossinline resultParser: (ResultSet) -> T): Iterator<T> {
    val resultSet = this
    return object : Iterator<T> {
        private var hasNext = resultSet.next()
        override fun hasNext(): Boolean {
            return hasNext
        }

        override fun next(): T {
            val value = resultParser(resultSet)
            hasNext = resultSet.next()
            return value
        }
    }
}