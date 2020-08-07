package fi.hsl.transitdata.rata_digitraffic.model.doi

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter

data class TripInfo(
        val dvjId: String,
        val routeId: String,
        val startDate: String,
        val startTime: String,
        val endTime: String,
        val directionId: Int,
        val startStopNumber: String,
        val endStopNumber: String,
        val commuterLineID: String
) {
    companion object {
        private const val ONE_DAY_IN_SECONDS = 86400L

        /**
         * Parses time in HH:mm:ss format to seconds
         * @param time
         * @return Time in seconds
         */
        private fun parseTime(time: String): Int {
            val parts = time.split(":").mapNotNull { it.toIntOrNull() }
            require(parts.size == 3) { "Invalid time format: $time" }
            return parts[0] * 60 * 60 + parts[1] * 60 + parts[2]
        }

        private fun getTimeAsLocalDateTime(date: String, time: Int): LocalDateTime {
            val timeAfterMidnight = time % ONE_DAY_IN_SECONDS //Time can be over 24 hours, e.g. 28:00:00 would be 4am
            val daysAfterDate = time / ONE_DAY_IN_SECONDS //Find actual date

            return LocalDate.parse(date, DateTimeFormatter.BASIC_ISO_DATE).plusDays(daysAfterDate).atTime(LocalTime.ofSecondOfDay(timeAfterMidnight))
        }
    }

    val startTimeAsLocalDateTime by lazy { getTimeAsLocalDateTime(startDate, parseTime(startTime)) }

    val endTimeAsLocalDateTime by lazy { getTimeAsLocalDateTime(startDate, parseTime(endTime)) }
}