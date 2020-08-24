package fi.hsl.transitdata.rata_digitraffic

import com.google.protobuf.ByteString
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import fi.hsl.common.mqtt.proto.Mqtt
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.transitdata.rata_digitraffic.model.digitraffic.Train
import fi.hsl.transitdata.rata_digitraffic.model.doi.StopPoint
import fi.hsl.transitdata.rata_digitraffic.model.doi.TripInfo
import org.apache.pulsar.client.api.*
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.mockito.Mockito
import java.net.URL
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.function.BiFunction



class MessageHandlerTest {


    lateinit var mockContext: PulsarApplicationContext
    lateinit var mockConsumer: Consumer<ByteArray>
    lateinit var mockTripUpdateProducer : Producer<ByteArray>
    lateinit var mockTrainCancellationProducer : Producer<ByteArray>
    lateinit var messageHandler : MessageHandler
    lateinit var doiStopMatcher: DoiStopMatcher
    lateinit var doiTripMatcher: DoiTripMatcher

    @Before
    fun before(){

        val mockTypeMessageBuilder = mock<TypedMessageBuilder<ByteArray>>{
            on{eventTime(any<Long>())} doReturn (it)
            on{property(any<String>(), any<String>())} doReturn(it)
            on{ value(any())} doReturn (it)
            on{sendAsync()} doReturn (CompletableFuture())
        }

        mockConsumer = mock<Consumer<ByteArray>>{
            on{acknowledgeAsync(any<MessageId>())} doReturn (CompletableFuture<Void>())
        }
        mockTripUpdateProducer = mock<Producer<ByteArray>>{
            on{newMessage()} doReturn (mockTypeMessageBuilder)
        }
        mockTrainCancellationProducer = mock<Producer<ByteArray>>{
            on{newMessage()} doReturn (mockTypeMessageBuilder)
        }
        doiStopMatcher = mock<DoiStopMatcher>{
            on{getStopPointForStationAndTrack(any<String>(), any<Int>())} doReturn StopPoint("1000", "dummy stop", 12.34, 56.78)
        }
        doiTripMatcher = mock<DoiTripMatcher>{
            on{matchTrainToTrip(any<Train>())} doReturn (TripInfo("dvjId", "routeId", "20201203", "00:01:02", "01:01:02", 1, "1000", "2000", "1234"))
        }
        mockContext = mock<PulsarApplicationContext>{
            on{consumer} doReturn (mockConsumer)
            on{producers} doReturn (mapOf("feedmessage-tripupdate" to mockTripUpdateProducer, "feedmessage-train-cancelled" to mockTrainCancellationProducer))
        }

        messageHandler = MessageHandler(mockContext, doiStopMatcher, doiTripMatcher)
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
        Mockito.verify(mockConsumer, Mockito.times(2)).acknowledgeAsync(any<MessageId>())
        Mockito.verify(mockTripUpdateProducer, Mockito.times(1)).newMessage()
    }


    @Test
    fun handleCancellationMessageTest(){
        val message = createMessage("cancelled_train.json")
        messageHandler.handleMessage(message)
        Mockito.verify(mockConsumer, Mockito.times(2)).acknowledgeAsync(any<MessageId>())
        Mockito.verify(mockTripUpdateProducer, Mockito.times(1)).newMessage()
        Mockito.verify(mockTrainCancellationProducer, Mockito.times(1)).newMessage()
    }
}