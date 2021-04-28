package fi.hsl.transitdata.rata_digitraffic

import com.google.protobuf.ByteString
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argThat
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import fi.hsl.common.mqtt.proto.Mqtt
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.common.transitdata.proto.InternalMessages
import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Station
import fi.hsl.transitdata.rata_digitraffic.model.doi.JourneyPatternStop
import fi.hsl.transitdata.rata_digitraffic.model.doi.StopPoint
import fi.hsl.transitdata.rata_digitraffic.model.doi.TripInfo
import org.apache.pulsar.client.api.*
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import java.net.URL
import java.time.ZoneId
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.function.BiFunction



class MessageHandlerTest {
    //Used for test data
    private val stationCodes = listOf("KKN", "TOL", "JRS", "MAS", "KLH", "EPO", "TRL", "KVH", "KNI", "KEA", "KIL", "LPV", "HPL", "PSL", "HKI")
    
    //Helper functions for generating mock data
    private fun generateStopPoints(count: Int): List<StopPoint> = (1..count).map { index -> StopPoint((1000 + index).toString(), "1", index.toDouble(), index.toDouble()) }

    private fun generateStations(count: Int): List<Station> = (1..count).map { index -> Station(true, stationCodes[(index - 1) % stationCodes.size], index.toDouble(), index.toDouble()) }

    private fun generateJourneyPatternStops(count: Int): List<JourneyPatternStop> = (1..count).map { index -> JourneyPatternStop("1", (1000 + index).toString(), index) }

    lateinit var mockContext: PulsarApplicationContext
    lateinit var mockConsumer: Consumer<ByteArray>
    lateinit var mockTripUpdateProducer : Producer<ByteArray>
    lateinit var mockTrainCancellationProducer : Producer<ByteArray>
    lateinit var messageHandler : MessageHandler
    lateinit var tripUpdateMockTypeMessageBuilder : TypedMessageBuilder<ByteArray>
    lateinit var trainCancellationMockTypeMessageBuilder : TypedMessageBuilder<ByteArray>

    @Before
    fun before() {
        tripUpdateMockTypeMessageBuilder = mock<TypedMessageBuilder<ByteArray>>{
            on{eventTime(any<Long>())} doReturn (it)
            on{property(any<String>(), any<String>())} doReturn(it)
            on{ value(any())} doReturn (it)
            on{sendAsync()} doReturn (CompletableFuture())
        }

        trainCancellationMockTypeMessageBuilder = mock<TypedMessageBuilder<ByteArray>>{
            on{eventTime(any<Long>())} doReturn (it)
            on{property(any<String>(), any<String>())} doReturn(it)
            on{ value(any())} doReturn (it)
            on{sendAsync()} doReturn (CompletableFuture())
        }

        mockConsumer = mock<Consumer<ByteArray>>{
            on{acknowledgeAsync(any<MessageId>())} doReturn (CompletableFuture<Void>())
        }
        mockTripUpdateProducer = mock<Producer<ByteArray>>{
            on{newMessage()} doReturn (tripUpdateMockTypeMessageBuilder)
        }
        mockTrainCancellationProducer = mock<Producer<ByteArray>>{
            on{newMessage()} doReturn (trainCancellationMockTypeMessageBuilder)
        }
        mockContext = mock<PulsarApplicationContext>{
            on{consumer} doReturn (mockConsumer)
            on{producers} doReturn (mapOf("feedmessage-tripupdate" to mockTripUpdateProducer, "feedmessage-train-cancelled" to mockTrainCancellationProducer))
        }

        val trips = listOf(TripInfo("1", "3001U", "20200822", "12:37:00", "13:18:00", 2, "1001", "1014", "U", "1"))
        val stops = generateStopPoints(15)
        val stations = generateStations(15)
        val journeyPatternStops = mapOf("1" to generateJourneyPatternStops(15))

        messageHandler = MessageHandler(mockContext, false, ZoneId.of("Europe/Helsinki"), Metadata(
            trips, stops, journeyPatternStops, stations
        ))
    }

    private fun createMessage(path : String) : Message<Any>{
        val classLoader = javaClass.classLoader
        val url: URL = classLoader.getResource(path)
        val content = Scanner(url.openStream(), "UTF-8").useDelimiter("\\A").next()
        val topic = "/topic/with/json/payload/#"
        val payload = content.toByteArray(charset("UTF-8"))
        val mapper: BiFunction<String, ByteArray, ByteArray> = createMapper()
        val mapped = mapper.apply(topic, payload)
        val mqttMessage = Mqtt.RawMessage.parseFrom(mapped)
        var pulsarMessage = mock<Message<Any>> {
            on{eventTime} doReturn (Date().time)
            on{data} doReturn (mqttMessage.toByteArray())
            on{messageId} doReturn (mock())
            on{getProperty("protobuf-schema")} doReturn ("mqtt-raw")
        }
        return pulsarMessage
    }

    private fun createMapper(): BiFunction<String, ByteArray, ByteArray> {
        return BiFunction { topic: String?, payload: ByteArray? ->
            val builder = Mqtt.RawMessage.newBuilder()
            val raw = builder
                    .setSchemaVersion(builder.schemaVersion)
                    .setTopic(topic)
                    .setPayload(ByteString.copyFrom(payload))
                    .build()
            raw.toByteArray()
        }
    }


    @Test
    fun handleNormalMessageTest(){
        //Build message
        val message = createMessage("confirmed_train.json")
        messageHandler.handleMessage(message)
        Mockito.verify(mockConsumer, Mockito.times(1)).acknowledgeAsync(any<MessageId>()) //Message should be acknowledged
        Mockito.verify(mockTripUpdateProducer, Mockito.times(1)).newMessage()
        Mockito.verify(trainCancellationMockTypeMessageBuilder).value(argThat{
            val tripCancellation = InternalMessages.TripCancellation.parseFrom(this)
            tripCancellation.status == InternalMessages.TripCancellation.Status.RUNNING

        })
        Mockito.verify(mockTrainCancellationProducer, Mockito.times(1)).newMessage()
    }


    @Test
    fun handleCancellationMessageTest(){
        val message = createMessage("cancelled_train.json")
        messageHandler.handleMessage(message)
        Mockito.verify(mockConsumer, Mockito.times(1)).acknowledgeAsync(any<MessageId>()) //Message should be acknowledged
        Mockito.verify(mockTripUpdateProducer, Mockito.times(1)).newMessage()
        Mockito.verify(trainCancellationMockTypeMessageBuilder).value(argThat{
            val tripCancellation = InternalMessages.TripCancellation.parseFrom(this)
            tripCancellation.status == InternalMessages.TripCancellation.Status.CANCELED

        })
        Mockito.verify(mockTrainCancellationProducer, Mockito.times(1)).newMessage()
    }
}