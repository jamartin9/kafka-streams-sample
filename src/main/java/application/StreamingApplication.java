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
import serialization.utils.Utility;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Kafka Streams streaming app
 */
public class StreamingApplication implements Runnable{
    // private stream
    // there should never be more than a single stream as the builder should merge them all together
    // so the 'app' stream can be merged with another apps stream for a larger topology
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
        // can be merge kstreams together and be converted into topology
        //KStreamBuilder builder = new KStreamBuilder();
        // create streams
        //this.streams = new KafkaStreams(builder, props);

        // build serializers/deserialzers
        //Map<String, Object> serdeProps = new HashMap<>();
        // auto serde
        //final Serde<CustomModel> autoModelViewSerde = Utility.getSerde(CustomModel.class);
        // known json pojo
        //final Deserializer<CustomModel> autoModelDeserializer = new JsonPOJODeserializer<>();
        //serdeProps.put("JsonPOJOClass", CustomModel.class);
        //autoModelDeserializer.configure(serdeProps, false);
        //final Serializer<CustomModel> autoModelSerializer = new JsonPOJOSerializer<>();
        //serdeProps.put("JsonPOJOClass", CustomModel.class);
        //autoModelSerializer.configure(serdeProps, false);
        try {
            final Serde<CustomModel> autoJsonViewSerde = Utility.getSerde(CustomModel.class, JsonPOJODeserializer.class, JsonPOJOSerializer.class);
            final Deserializer<CustomModel> deModel = Utility.getDeserializer(CustomModel.class, JsonPOJODeserializer.class);
            final Serializer<CustomModel> seModel = Utility.getSerializer(CustomModel.class, JsonPOJOSerializer.class);

            TopologyBuilder topologyBuilder = new TopologyBuilder();
            // create source node named SOURCE, deserialize the keys as strings, deserialize the values as POJO, read from the src-topic topic
            topologyBuilder.addSource("SOURCE", Utility.getStringDeserializer(), deModel, "src-topic")
                    // set name as process and source as parent
                    .addProcessor("PROCESS", MyProcessor::new, "SOURCE")
                    // add sink node named SINK, that serializes keys as string and values as POJO json from processor
                    .addSink("SINK", "patterns", Utility.getStringSerializer(), seModel, "PROCESS");

            //Use the topologyBuilder and streamingConfig to start the kafka streams process
            this.streams = new KafkaStreams(topologyBuilder, props);
            // set handler
            this.streams.setUncaughtExceptionHandler((t, e) -> {
                // examine exception
                System.out.println("Uncaught Exception: "+e.getMessage());
            });

            // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }

    }

    // give streams from app
    public KafkaStreams getStreams(){
        return this.streams;
    }

    @Override
    public void run() {
        // let it rip
        this.streams.start();
        // here is where dynamic reloading would take place
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
