package fi.hsl.transitdata.rata_digitraffic

import com.google.transit.realtime.GtfsRealtime
import fi.hsl.common.gtfsrt.FeedMessageFactory
import fi.hsl.common.transitdata.RouteIdUtils
import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.TimetableRow
import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Train
import fi.hsl.transitdata.rata_digitraffic.model.doi.JourneyPatternStop
import mu.KotlinLogging
import java.time.Instant
import kotlin.math.abs

class TripUpdateBuilder(
    private val platformChangesEnabled: Boolean,
    private val doiStopMatcher: DoiStopMatcher,
    private val doiTripMatcher: DoiTripMatcher,
    private val journeyPatternStopsById: Map<String, List<JourneyPatternStop>>
) {
    private val log = KotlinLogging.logger {}

    fun buildTripUpdate(train: Train, timestamp: Instant): GtfsRealtime.FeedMessage? {
        val tripInfo = doiTripMatcher.matchTrainToTrip(train) ?: return null

        val trip = GtfsRealtime.TripDescriptor.newBuilder()
                .setRouteId(RouteIdUtils.normalizeRouteId(tripInfo.routeId))
                .setStartDate(tripInfo.startDate)
                .setStartTime(tripInfo.startTime)
                .setDirectionId(tripInfo.directionId - 1)
                .setScheduleRelationship(
                    if (train.cancelledOrDeleted)
                        GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED
                    else
                        GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED
                )
                .build()

        val tripUpdateBuilder = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(trip)
                .setTimestamp(timestamp.epochSecond)

        //Train was cancelled, no need to add stop time updates
        if (trip.scheduleRelationship == GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED) {
            return FeedMessageFactory.createDifferentialFeedMessage(tripInfo.dvjId, tripUpdateBuilder.build(), timestamp.epochSecond)
        }

        val commercialStopRows = train.timeTableRows.filter { timetableRow -> isCommercialStop(timetableRow) }

        val stopTimeUpdates = mergeTimetableRows(commercialStopRows).mapIndexedNotNull { stopIndex, timetableRow ->
            val stationShortCode = (timetableRow.arrival?.stationShortCode ?: timetableRow.departure?.stationShortCode)!!
            val track = (timetableRow.arrival?.commercialTrack ?: timetableRow.departure?.commercialTrack)?.toIntOrNull()

            val stopId = getStopNumber(stationShortCode, track, tripInfo.journeyPatternId, stopIndex)

            val stopTimeUpdateBuilder = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                    .setStopId(stopId)

            if (!timetableRow.hasRealtimeData) {
                stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA).build()
            } else if (timetableRow.isCancelled) {
                stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED).build()
            } else  {
                val arrival = timetableRow.arrival?.let { timetableRowToStopTimeEvent(it) }
                val departure = timetableRow.departure?.let { timetableRowToStopTimeEvent(it) }

                stopTimeUpdateBuilder
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .setArrival((arrival ?: departure)!!) //Use departure data for arrival (and vice versa) if no arrival data is not available
                        .setDeparture((departure ?: arrival)!!)
                        .build()
            }
        }

        tripUpdateBuilder.addAllStopTimeUpdate(stopTimeUpdates)

        return FeedMessageFactory.createDifferentialFeedMessage(tripInfo.dvjId, tripUpdateBuilder.build(), timestamp.epochSecond)
    }

    private fun getStopNumber(stationShortCode: String, track: Int?, journeyPatternId: String, timetableRowIndex: Int): String? {
        return if (platformChangesEnabled) {
            //If platform changes feature is enabled, try to find stop ID for the new platform

            //Match station to DOI stop. If the track is unknown, use track 1 to match to any possible stop
            val doiStop = doiStopMatcher.getStopPointForStationAndTrack(stationShortCode, track ?: 1)
            if (doiStop == null) {
                log.warn("No stop found for station {} and track {}", stationShortCode, track)
            }
            doiStop?.stopNumber
        } else {
            //If platform changes are not enabled, use stop ID from the static schedule

            //List of stop IDs within the station
            val stopsWithinStation = doiStopMatcher.getStopsWithinStation(stationShortCode)
            val journeyPatternStops = journeyPatternStopsById[journeyPatternId].orEmpty()
            val stop = journeyPatternStops
                .filter { journeyPatternStop -> journeyPatternStop.stopNumber in stopsWithinStation }
                //Same train can go through same station multiple times through different tracks
                //-> Try to find stop with same sequence number as the timetable row index
                .minBy { journeyPatternStop -> abs((timetableRowIndex + 1) - journeyPatternStop.sequenceNumber) }
            if (stop == null) {
                log.warn("No stop found for station {} with timetable row index {} from journey pattern {}", stationShortCode, timetableRowIndex, journeyPatternId)
            }

            stop?.stopNumber
        }
    }

    private fun isCommercialStop(timetableRow: TimetableRow) = timetableRow.trainStopping && timetableRow.commercialStop == true

    private fun timetableRowToStopTimeEvent(timetableRow: TimetableRow): GtfsRealtime.TripUpdate.StopTimeEvent {
        val builder = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                .setTime((timetableRow.actualTime ?: timetableRow.liveEstimateTime)!!.toEpochSecond())

        if (timetableRow.actualTime != null) {
            builder.uncertainty = 0
        }

        return builder.build()
    }

    private fun mergeTimetableRows(timetableRows: Collection<TimetableRow>): Collection<MergedTimetableRow> {
        val mergedTimetableRows = mutableListOf<MergedTimetableRow>()

        var mergedTimetableRow = MergedTimetableRow()
        for (timetableRow in timetableRows) {
            if (timetableRow.type == TimetableRow.TimetableRowType.DEPARTURE) {
                mergedTimetableRows += mergedTimetableRow.copy(departure = timetableRow)
            } else if (timetableRow.type == TimetableRow.TimetableRowType.ARRIVAL) {
                mergedTimetableRow = mergedTimetableRow.copy(arrival = timetableRow)
            }
        }
        mergedTimetableRows += mergedTimetableRow

        return mergedTimetableRows.toList()
    }

    private data class MergedTimetableRow(val arrival: TimetableRow? = null, val departure: TimetableRow? = null) {
        val hasArrival = arrival != null
        val hasDeparture = departure != null

        //Timetable row is cancelled if both arrival and departure are cancelled or if other is cancelled and other is null (for first and last rows)
        val isCancelled = (arrival?.cancelled ?: true) && (departure?.cancelled ?: true)

        //True if there are estimates or observed times available
        val hasRealtimeData = arrival?.liveEstimateTime != null
                || arrival?.actualTime != null
                || departure?.liveEstimateTime != null
                || departure?.actualTime != null
    }
}