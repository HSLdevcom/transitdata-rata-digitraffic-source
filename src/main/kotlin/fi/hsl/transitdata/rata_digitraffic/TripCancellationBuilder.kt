package fi.hsl.transitdata.rata_digitraffic

import fi.hsl.common.transitdata.proto.InternalMessages.TripCancellation
import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Train

class TripCancellationBuilder(private val doiTripMatcher: DoiTripMatcher) {

    fun buildTripCancellation(train: Train): TripCancellation? {
        val tripInfo = doiTripMatcher.matchTrainToTrip(train) ?: return null

        val builder = TripCancellation.newBuilder()
                .setTripId(tripInfo.dvjId)
                .setRouteId(tripInfo.routeId)
                .setDirectionId(tripInfo.directionId)
                .setStartDate(tripInfo.startDate)
                .setStartTime(tripInfo.startTime)
                .setStatus(TripCancellation.Status.CANCELED)

        //Version number is defined in the proto file as default value but we still need to set it since it's a required field
        builder.schemaVersion = builder.schemaVersion

        return builder.build()
    }
}