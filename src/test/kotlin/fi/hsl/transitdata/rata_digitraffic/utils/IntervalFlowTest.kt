package fi.hsl.transitdata.rata_digitraffic.utils

import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertArrayEquals

import org.junit.Assert.assertTrue
import org.junit.Test
import java.time.Duration
import kotlin.system.measureTimeMillis

class IntervalFlowTest {
    @InternalCoroutinesApi
    @Test
    fun `Test interval flow emits items correctly`() {
        val flow = intervalFlow(Duration.ofSeconds(1))
        val output = mutableListOf<Long>()

        val timeMillis = runBlocking {
            measureTimeMillis {
                flow.take(5).collect { value -> output += value }
            }
        }

        assertTrue(timeMillis > 4000)
        assertArrayEquals(arrayOf(0L, 1L, 2L, 3L, 4L), output.toTypedArray())
    }
}