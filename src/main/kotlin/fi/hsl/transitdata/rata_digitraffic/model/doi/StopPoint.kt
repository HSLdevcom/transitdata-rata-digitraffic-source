package fi.hsl.transitdata.rata_digitraffic.model.doi

import fi.hsl.transitdata.rata_digitraffic.model.LatLng

data class StopPoint(
        val stopNumber: String,
        val designation: String?,
        val latitude: Double,
        val longitude: Double
) {
    val location = LatLng(latitude, longitude)

    /**
     * Range of tracks that this stop point covers
     */
    fun tracks(): IntRange? {
        val tracks = designation?.split("-")?.mapNotNull { it.toIntOrNull() }
        return when (tracks?.size) {
            1 -> {
                tracks[0]..tracks[0]
            }
            2 -> {
                tracks[0]..tracks[1]
            }
            else -> {
                null
            }
        }
    }
}