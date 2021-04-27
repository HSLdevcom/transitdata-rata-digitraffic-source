package fi.hsl.transitdata.rata_digitraffic

import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Station
import fi.hsl.transitdata.rata_digitraffic.model.doi.StopPoint
import mu.KotlinLogging
import kotlin.math.abs
import kotlin.math.min

class DoiStopMatcher(
    private val doiStops: Collection<StopPoint>,
    private val stations: Collection<Station>
) {
    companion object {
        private const val MAX_DISTANCE_FROM_CLOSEST = 200 //200 meters
    }

    private val stationToDoiStops = stations.associate { station ->
        val distanceToClosest = doiStops.map { stopPoint -> stopPoint.location.distanceTo(station.location) }.min()
        return@associate station to doiStops.filter { stopPoint ->
            distanceToClosest != null && stopPoint.location.distanceTo(station.location) < (distanceToClosest + MAX_DISTANCE_FROM_CLOSEST)
        }
    }

    private val log = KotlinLogging.logger {}

    private val stationsByShortCode = stations.associateBy(keySelector = { station -> station.stationShortCode })

    fun checkIfStationContainsStop(stationShortCode: String, stopNumber: String): Boolean {
        return stopNumber in getStopsWithinStation(stationShortCode)
    }

    fun getStopPointForStationAndTrack(stationShortCode: String, track: Int): StopPoint? {
        if (stationShortCode !in stationsByShortCode) {
            log.warn("No station found by short code {}", stationShortCode)
            return null
        }

        return stationToDoiStops[stationsByShortCode[stationShortCode]].orEmpty().minBy { stopPoint ->
            val tracks = stopPoint.tracks()
            if (tracks == null) {
                log.warn("Stop point {} (designation: {}) did not contain any tracks", stopPoint.stopNumber, stopPoint.designation)
                return@minBy Int.MAX_VALUE
            }

            if (tracks.contains(track)) {
                return@minBy 0
            } else {
                return@minBy min(abs(tracks.first - track), abs(tracks.last - track))
            }
        }
    }

    /**
     * @return Set of stop codes that are within the station
     */
    fun getStopsWithinStation(stationShortCode: String): Set<String> {
        return stationsByShortCode[stationShortCode]
            ?.let { station -> stationToDoiStops[station] }
            ?.map { it.stopNumber }
            ?.toSet() ?: emptySet()
    }
}