package fi.hsl.transitdata.rata_digitraffic

import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.TimetableRow
import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Train
import fi.hsl.transitdata.rata_digitraffic.model.doi.TripInfo
import fi.hsl.transitdata.rata_digitraffic.source.DoiSource
import kotlinx.coroutines.*
import kotlinx.coroutines.time.delay
import mu.KotlinLogging
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId

class DoiTripMatcher(private val zoneId: ZoneId, private val doiQueryFutureDays: Int, private val doiSource : DoiSource, private val doiStopMatcher: DoiStopMatcher) {
    private val log = KotlinLogging.logger {}
    private lateinit var doiTrips: Collection<TripInfo>

    companion object{
        fun newInstance(zoneId: ZoneId, queryFutureDays: Int, doiSource : DoiSource, doiStopMatcher: DoiStopMatcher) : DoiTripMatcher{
            val doiTripMatcher = DoiTripMatcher(zoneId, queryFutureDays, doiSource, doiStopMatcher)
            GlobalScope.launch(Dispatchers.IO){
                doiTripMatcher.resetCollections()
                while(true){
                    val tomorrow = LocalDateTime.now().plusDays(1).withHour(0).withMinute(0).withSecond(0)
                    val now = LocalDateTime.now()
                    delay(Duration.between(now, tomorrow))
                    doiTripMatcher.resetCollections()
                }
            }.start()
            return doiTripMatcher
        }
    }

    suspend fun resetCollections(){
        doiTrips = doiSource.getTrainTrips(LocalDate.now(), doiQueryFutureDays.toLong())
    }

    fun matchTrainToTrip(train: Train): TripInfo? {
        val possibleTrips = doiTrips.filter { trip ->
            fun matches(timetableRow: TimetableRow, stopNumber: String, scheduledTime: LocalDateTime): Boolean {
                return doiStopMatcher.checkIfStationContainsStop(timetableRow.stationShortCode, stopNumber)
                        && timetableRow.scheduledTime.withZoneSameInstant(zoneId).toLocalDateTime() == scheduledTime
            }

            val firstTimetableRow = train.timeTableRows.first()
            val lastTimetableRow = train.timeTableRows.last()

            val sameFirstOrLastTimetableRow = matches(firstTimetableRow, trip.startStopNumber, trip.startTimeAsLocalDateTime) || matches(lastTimetableRow, trip.endStopNumber, trip.endTimeAsLocalDateTime)

            //Check that the train has same commuter line ID (A, P, K, etc.) and either the first or last timetable row has same station and scheduled time
            return@filter train.commuterLineID == trip.commuterLineID && sameFirstOrLastTimetableRow
        }

        if (possibleTrips.isEmpty()) {
            log.warn("No trips found for train {} ({}) departing at {} from {}",
                    train.trainNumber,
                    train.commuterLineID ?: "",
                    train.timeTableRows.first().scheduledTime,
                    train.timeTableRows.first().stationShortCode)
            return null
        }

        if (possibleTrips.size > 1) {
            log.warn("More than 1 possible trip found for train {}", train.trainNumber)
        }

        return possibleTrips.random()
    }
}