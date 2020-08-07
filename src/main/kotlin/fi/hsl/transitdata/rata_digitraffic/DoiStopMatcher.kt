package fi.hsl.transitdata.rata_digitraffic

import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Station
import fi.hsl.transitdata.rata_digitraffic.model.doi.StopPoint
import fi.hsl.transitdata.rata_digitraffic.source.DoiSource
import fi.hsl.transitdata.rata_digitraffic.source.RataDigitrafficStationSource
import kotlinx.coroutines.*
import kotlinx.coroutines.time.delay
import mu.KotlinLogging
import java.sql.Connection
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import kotlin.math.abs
import kotlin.math.min

class DoiStopMatcher(private val doiSource : DoiSource, private val stations: Collection<Station>) {

    companion object {
        private const val MAX_DISTANCE_FROM_CLOSEST = 200 //200 meters

        fun newInstance(doiSource : DoiSource, stations: Collection<Station>) : DoiStopMatcher{
            val doiStopMatcher = DoiStopMatcher(doiSource, stations)
            doiStopMatcher.resetCollections()
            GlobalScope.launch(Dispatchers.IO){
                while(true){
                    val tomorrow = LocalDateTime.now().plusDays(1).withHour(0).withMinute(0).withSecond(0)
                    val now = LocalDateTime.now()
                    delay(Duration.between(now, tomorrow))
                    doiStopMatcher.resetCollectionsAsync()
                }
            }.start()
            return doiStopMatcher
        }
    }

    lateinit var doiStops: Collection<StopPoint>

    private lateinit var stationToDoiStops: Map<Station, List<StopPoint>>

    fun resetCollections(){
        log.debug("Reset collections")
        runBlocking{
            withContext(Dispatchers.IO){
                resetCollectionsAsync()
            }
        }
    }


    suspend fun resetCollectionsAsync(){
        log.debug("Reset collections async")
        doiStops = doiSource.getStopPointsForRailwayStations(LocalDate.now())
        stationToDoiStops =
            stations.associate { station ->
                val distanceToClosest = doiStops.map { stopPoint -> stopPoint.location.distanceTo(station.location) }.min()
                return@associate station to doiStops.filter { stopPoint ->
                    distanceToClosest != null && stopPoint.location.distanceTo(station.location) < (distanceToClosest + MAX_DISTANCE_FROM_CLOSEST)
                }
            }
        log.debug("Reset collections async done")
    }

    private val log = KotlinLogging.logger {}

    private val stationsByShortCode = stations.associateBy(keySelector = { station -> station.stationShortCode })



    fun checkIfStationContainsStop(stationShortCode: String, stopNumber: String): Boolean {
        val doiStops = stationsByShortCode[stationShortCode]?.let { station -> stationToDoiStops[station] }
        return doiStops?.any { stopPoint -> stopPoint.stopNumber == stopNumber } ?: false
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


}