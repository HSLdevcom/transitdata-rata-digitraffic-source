package fi.hsl.transitdata.rata_digitraffic.model.digitraffic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import fi.hsl.transitdata.rata_digitraffic.utils.JsonHelper
import org.junit.Assert.*
import org.junit.Test
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZonedDateTime

class TrainTest {
    @Test
    fun `Test parsing train from JSON`() {
        javaClass.classLoader.getResourceAsStream("train_1.json").use { stream ->
            val trains: List<Train> = JsonHelper.parseList(stream)

            assertEquals(1, trains.size)

            val train = trains[0]

            assertEquals(1, train.trainNumber)
            assertEquals(LocalDate.parse("2020-07-15"), train.departureDate)
            assertEquals("IC", train.trainType)
            assertEquals("Long-distance", train.trainCategory)
            assertEquals("", train.commuterLineID)
            assertFalse(train.runningCurrently)
            assertFalse(train.cancelled)
            assertEquals("REGULAR", train.timetableType)
            assertEquals(134, train.timeTableRows.size)

            val firstTimetableRow = train.timeTableRows[0]

            assertEquals("HKI", firstTimetableRow.stationShortCode)
            assertEquals(TimetableRow.TimetableRowType.DEPARTURE, firstTimetableRow.type)
            assertTrue(firstTimetableRow.trainStopping)
            assertTrue(firstTimetableRow.commercialStop!!)
            assertEquals("8", firstTimetableRow.commercialTrack!!)
            assertFalse(firstTimetableRow.cancelled)
            assertEquals(ZonedDateTime.parse("2020-07-15T03:52:00.000Z").withZoneSameInstant(ZoneId.of("UTC")), firstTimetableRow.scheduledTime)
            assertNull(firstTimetableRow.liveEstimateTime)
            assertEquals(ZonedDateTime.parse("2020-07-15T03:52:36.000Z").withZoneSameInstant(ZoneId.of("UTC")), firstTimetableRow.actualTime)
        }
    }
}