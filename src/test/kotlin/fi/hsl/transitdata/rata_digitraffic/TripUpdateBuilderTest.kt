package fi.hsl.transitdata.rata_digitraffic

import com.google.transit.realtime.GtfsRealtime
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.TimetableRow
import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Train
import fi.hsl.transitdata.rata_digitraffic.model.doi.JourneyPatternStop
import fi.hsl.transitdata.rata_digitraffic.model.doi.StopPoint
import fi.hsl.transitdata.rata_digitraffic.model.doi.TripInfo
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import java.time.Instant
import java.time.LocalDate
import java.time.ZonedDateTime

class TripUpdateBuilderTest {
    lateinit var tripUpdateBuilder: TripUpdateBuilder

    @Before
    fun setup() {
        val mockStopMatcher = mock<DoiStopMatcher> {
            on { checkIfStationContainsStop("A", "1") } doReturn(true)
            on { checkIfStationContainsStop("B", "1") } doReturn(true)
            on { getStopPointForStationAndTrack("A", 1) } doReturn(StopPoint("1", "1", 1.0, 1.0))
            on { getStopPointForStationAndTrack("B", 1) } doReturn(StopPoint("2", "1", 1.0, 1.0))
            on { getStopsWithinStation("A") } doReturn(setOf("1"))
            on { getStopsWithinStation("B") } doReturn(setOf("2"))
        }
        val mockTripMatcher = mock<DoiTripMatcher> {
            on { matchTrainToTrip(any()) } doReturn(TripInfo("1", "3001U", "20210101", "10:00:00", "10:30:00", 2, "1", "2", "U", "1"))
        }

        tripUpdateBuilder = TripUpdateBuilder(
            false,
            mockStopMatcher,
            mockTripMatcher,
            mapOf(
                "1" to listOf(
                    JourneyPatternStop("1", "1", 1),
                    JourneyPatternStop("1", "2", 2)
                )
            )
        )
    }

    @Test
    fun `Test building trip update with cancellation`() {
        val tripUpdate = tripUpdateBuilder.buildTripUpdate(
            Train(
                7,
                LocalDate.now(),
            "HL",
            "Commuter",
            "A",
            false,
            true,
            false,
            "REGULAR",
            listOf(
                TimetableRow("A", TimetableRow.TimetableRowType.DEPARTURE, true, true, "1", true, ZonedDateTime.now(), null, null, false),
                TimetableRow("B", TimetableRow.TimetableRowType.ARRIVAL, true, true, "1", true, ZonedDateTime.now().plusMinutes(10), null, null, false)
            )),
            Instant.now()
        )

        assertEquals(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED, tripUpdate!!.getEntity(0).tripUpdate.trip.scheduleRelationship)
    }

    @Test
    fun `Test building trip update`() {
        val tripUpdate = tripUpdateBuilder.buildTripUpdate(
            Train(
                7,
                LocalDate.now(),
                "HL",
                "Commuter",
                "A",
                false,
                false,
                false,
                "REGULAR",
                listOf(
                    TimetableRow("A", TimetableRow.TimetableRowType.DEPARTURE, true, true, "1", true, ZonedDateTime.now(), null, null, false),
                    TimetableRow("B", TimetableRow.TimetableRowType.ARRIVAL, true, true, "1", true, ZonedDateTime.now().plusMinutes(10), null, null, false)
                )),
            Instant.now()
        )

        assertEquals(GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED, tripUpdate!!.getEntity(0).tripUpdate.trip.scheduleRelationship)
        assertEquals(2, tripUpdate.getEntity(0).tripUpdate.stopTimeUpdateCount)
    }
}