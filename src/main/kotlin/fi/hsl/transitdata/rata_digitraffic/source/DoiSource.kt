package fi.hsl.transitdata.rata_digitraffic.source

import fi.hsl.transitdata.rata_digitraffic.model.doi.StopPoint
import fi.hsl.transitdata.rata_digitraffic.model.doi.TripInfo
import fi.hsl.transitdata.rata_digitraffic.utils.iterator
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.sql.Connection
import java.time.LocalDate

class DoiSource(private val connection: Connection) {
    companion object {
        private const val STOP_POINT_QUERY = """
            SELECT DISTINCT
                SP.Name AS name,
                SP.Designation AS track,
                CONVERT(CHAR(7), JPP.Number) AS stop_number,
                JPP.LocationNorthingCoordinate as latitude,
                JPP.LocationEastingCoordinate  as longitude
            FROM ptDOI4_Community.dbo.StopPoint AS SP
            LEFT JOIN ptDOI4_Community.dbo.StopArea AS SA ON SP.IsIncludedInStopAreaGid = SA.Gid 
            LEFT JOIN ptDOI4_Community.dbo.JourneyPatternPoint AS JPP ON JPP.Gid = SP.IsJourneyPatternPointGid
            WHERE SA.TypeCode = 'RAILWSTN' 
                AND (SP.ExistsFromDate <= ? AND (SP.ExistsUptoDate IS NULL OR SP.ExistsUptoDate > ?))
                AND (SA.ExistsFromDate <= ? AND (SA.ExistsUptoDate IS NULL OR SA.ExistsUptoDate > ?))
                AND (JPP.ExistsFromDate <= ? AND (JPP.ExistsUptoDate IS NULL OR JPP.ExistsUptoDate > ?))
        """

        private const val TRIP_QUERY = """
            SELECT
                DISTINCT CONVERT(CHAR(16), DVJ.Id) AS dvj_id,
                KVV.StringValue AS route,
                L.Designation AS commuter_line_id,
                DOL.DirectionCode AS direction,
                CONVERT(CHAR(8), DVJ.OperatingDayDate, 112) AS operating_day,
                RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', VJ.PlannedStartOffsetDateTime)))), 2) + ':' + RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', VJ.PlannedStartOffsetDateTime)) - ((DATEDIFF(HOUR, '1900-01-01', VJ.PlannedStartOffsetDateTime) * 60)))), 2) + ':00' AS start_time,
                RIGHT('0' + (CONVERT(VARCHAR(2), (DATEDIFF(HOUR, '1900-01-01', VJ.PlannedEndOffsetDateTime )))), 2) + ':' + RIGHT('0' + CONVERT(VARCHAR(2), ((DATEDIFF(MINUTE, '1900-01-01', VJ.PlannedEndOffsetDateTime )) - ((DATEDIFF(HOUR, '1900-01-01', VJ.PlannedEndOffsetDateTime ) * 60)))), 2) + ':00' AS end_time,
                CONVERT(CHAR(7), START_JPP.Number) AS start_stop_number,
                CONVERT(CHAR(7), END_JPP.Number) AS end_stop_number
            FROM
                ptDOI4_Community.dbo.DatedVehicleJourney AS DVJ
            LEFT JOIN ptDOI4_Community.dbo.VehicleJourney AS VJ ON
                (DVJ.IsBasedOnVehicleJourneyId = VJ.Id)
            LEFT JOIN ptDOI4_Community.dbo.VehicleJourneyTemplate AS VJT ON
                (DVJ.IsBasedOnVehicleJourneyTemplateId = VJT.Id)
            LEFT JOIN ptDOI4_Community.dbo.DirectionOfLine AS DOL ON
                VJT.IsWorkedOnDirectionOfLineGid = DOL.Gid
            LEFT JOIN ptDOI4_Community.dbo.Line AS L ON
                DOL.IsOnLineId = L.Id 
            LEFT JOIN ptDOI4_Community.T.KeyVariantValue AS KVV ON
                (KVV.IsForObjectId = VJ.Id)
            LEFT JOIN ptDOI4_Community.dbo.KeyVariantType AS KVT ON
                (KVT.Id = KVV.IsOfKeyVariantTypeId)
            LEFT JOIN ptDOI4_Community.dbo.KeyType AS KT ON
                (KT.Id = KVT.IsForKeyTypeId)
            LEFT JOIN ptDOI4_Community.dbo.ObjectType AS OT ON
                (KT.ExtendsObjectTypeNumber = OT.Number)
            LEFT JOIN ptDOI4_Community.dbo.JourneyPatternPoint AS START_JPP ON
                (VJT.StartsAtJourneyPatternPointGid = START_JPP.Gid)
            LEFT JOIN ptDOI4_Community.dbo.JourneyPatternPoint AS END_JPP ON
                (VJT.EndsAtJourneyPatternPointGid = END_JPP.Gid)
            WHERE
                (KT.Name = 'JoreIdentity'
                    OR KT.Name = 'JoreRouteIdentity'
                    OR KT.Name = 'RouteName')
                AND OT.Name = 'VehicleJourney'
                AND VJT.IsWorkedOnDirectionOfLineGid IS NOT NULL
                AND DVJ.OperatingDayDate >= ?
                AND DVJ.OperatingDayDate < ?
                AND DVJ.IsReplacedById IS NULL
                AND VJT.TransportModeCode = 'TRAIN'
        """
    }

    suspend fun getStopPointsForRailwayStations(date: LocalDate): List<StopPoint> = withContext(Dispatchers.IO) {
        connection.prepareStatement(STOP_POINT_QUERY).use { statement ->
            (1..6).forEach { i -> statement.setString(i, date.toString()) }
            val results = statement.executeQuery()

            return@use results.iterator { row ->
                        StopPoint(
                            row.getString("stop_number"),
                            row.getString("track"),
                            row.getDouble("latitude"),
                            row.getDouble("longitude")
                        )
                    }
                    .asSequence()
                    .toList()
        }
    }

    suspend fun getTrainTrips(date: LocalDate, futureDays: Long): List<TripInfo> = withContext(Dispatchers.IO) {
        connection.prepareStatement(TRIP_QUERY).use { statement ->
            statement.setString(1, date.toString())
            statement.setString(2, date.plusDays(futureDays).toString())
            val results = statement.executeQuery()

            return@use results.iterator { row ->
                        TripInfo(
                            row.getString("dvj_id"),
                            row.getString("route"),
                            row.getString("operating_day"),
                            row.getString("start_time"),
                            row.getString("end_time"),
                            row.getInt("direction"),
                            row.getString("start_stop_number"),
                            row.getString("end_stop_number"),
                            row.getString("commuter_line_id")
                        )
                    }
                    .asSequence()
                    .toList()
        }
    }
}