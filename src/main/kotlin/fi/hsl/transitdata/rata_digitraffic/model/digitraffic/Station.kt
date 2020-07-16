package fi.hsl.transitdata.rata_digitraffic.model.digitraffic

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import fi.hsl.transitdata.rata_digitraffic.model.LatLng

@JsonIgnoreProperties(ignoreUnknown = true)
data class Station(
        val passengerTraffic: Boolean,
        val stationShortCode: String,
        val latitude: Double,
        val longitude: Double
) {
    val location = LatLng(latitude, longitude)
}