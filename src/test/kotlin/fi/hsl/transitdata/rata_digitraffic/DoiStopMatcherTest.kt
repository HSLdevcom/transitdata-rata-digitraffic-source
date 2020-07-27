package fi.hsl.transitdata.rata_digitraffic

import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Station
import fi.hsl.transitdata.rata_digitraffic.model.doi.StopPoint

import org.junit.Assert.*
import org.junit.Test

class DoiStopMatcherTest {
    @Test
    fun `Stop point is found with station short code and track number`() {
        val doiStops = listOf(
                StopPoint("1", "1-3", 0.0, 0.0),
                StopPoint("2", "4-5", 0.0, 0.0),
                StopPoint("3", "1", 1.0, 1.0),
                StopPoint("4", "2", 1.0, 1.0)
        )

        val stations = listOf(
                Station(true, "A", 0.0, 0.0),
                Station(true, "B", 1.0, 1.0)
        )

        val doiStopMatcher = DoiStopMatcher(doiStops, stations)

        val stationATrack1 = doiStopMatcher.getStopPointForStationAndTrack("A", 1)
        val stationBTrack2 = doiStopMatcher.getStopPointForStationAndTrack("B", 2)

        assertTrue(stationATrack1 != null)
        assertEquals("1", stationATrack1!!.stopNumber)
        assertEquals("1-3", stationATrack1.designation)

        assertTrue(stationBTrack2 != null)
        assertEquals("4", stationBTrack2!!.stopNumber)
        assertEquals("2", stationBTrack2.designation)
    }
}