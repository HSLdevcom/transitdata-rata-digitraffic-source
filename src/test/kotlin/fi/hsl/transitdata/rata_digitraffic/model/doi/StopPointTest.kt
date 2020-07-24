package fi.hsl.transitdata.rata_digitraffic.model.doi

import org.hamcrest.CoreMatchers
import org.junit.Assert.*
import org.junit.Test

class StopPointTest {
    @Test
    fun `Test track range contains correct tracks`() {
        val stopPoint1 = StopPoint("1", "1", 0.0, 0.0)
        val tracks1 = stopPoint1.tracks()

        assertNotNull(tracks1)
        assertThat(tracks1!!, CoreMatchers.hasItem(1))

        val stopPoint2 = StopPoint("2", "2-3", 0.0, 0.0)
        val tracks2 = stopPoint2.tracks()

        assertNotNull(tracks2)
        assertThat(tracks2!!, CoreMatchers.hasItems(2, 3))
    }
}