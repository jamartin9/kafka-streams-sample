package application;

import com.fasterxml.jackson.databind.JsonNode;
import models.internal.CustomModel;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import processors.MyProcessor;
import serialization.deserializers.JsonPOJODeserializer;
import serialization.serializers.JsonPOJOSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by jam on 4/28/17.
 */
public class StreamingApplication implements Runnable{
    // private stream
    private KafkaStreams streams;

    public StreamingApplication(String AppID, String KafkaServer){

        Properties props = new Properties();
        // stream specific config
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaServer);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // consumer config settings
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // use this for higher level api
        // can be merged with topology
        //KStreamBuilder builder = new KStreamBuilder();

        // build serializers/deserialzers
        Map<String, Object> serdeProps = new HashMap<>();

        // auto serde
        final Serde<CustomModel> autoModelViewSerde = Serdes.serdeFrom(CustomModel.class);

        // known json pojo
        final Deserializer<CustomModel> autoModelDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", CustomModel.class);
        autoModelDeserializer.configure(serdeProps, false);
        final Serializer<CustomModel> autoModelSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", CustomModel.class);
        autoModelSerializer.configure(serdeProps, false);
        final Serde<CustomModel> autoJsonViewSerde = Serdes.serdeFrom(autoModelSerializer, autoModelDeserializer);

        // auto json
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonViewSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        StringDeserializer stringDeserializer = new StringDeserializer();
        StringSerializer stringSerializer = new StringSerializer();


        // create streams
        //this.streams = new KafkaStreams(builder, props);

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        // create source node named SOURCE, deserialize the keys as strings, deserialize the values as POJO, read from the src-topic topic
        topologyBuilder.addSource("SOURCE", stringDeserializer, autoModelDeserializer, "src-topic")

                .addProcessor("PROCESS", MyProcessor::new, "SOURCE")
                .addProcessor("PROCESS2", MyProcessor::new, "PROCESS")
                .addProcessor("PROCESS3", MyProcessor::new, "PROCESS")
                // add sink node named SINK, that serializes keys as string and values as POJO json from processor 2
                .addSink("SINK", "patterns", stringSerializer, autoModelSerializer, "PROCESS2");

        //Use the topologyBuilder and streamingConfig to start the kafka streams process
        this.streams = new KafkaStreams(topologyBuilder, props);
        // set handler
        this.streams.setUncaughtExceptionHandler((t, e) -> {
            // examine exception
            System.out.println("Uncaught Exception: "+e.getMessage());
        });

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    @Override
    public void run() {
        // let it rip
        this.streams.start();
        while(true){
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
                // usually the stream application would be running forever
                // close up
                this.streams.close();
            }
        }

    }
}
