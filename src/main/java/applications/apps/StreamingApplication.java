package applications.apps;

import applications.config.DefaultAppConfig;
import models.internal.CustomModel;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.TopologyBuilder;
import topologies.processors.MyProcessor;
import serialization.deserializers.JsonPOJODeserializer;
import serialization.serializers.JsonPOJOSerializer;
import serialization.utils.Utility;

import java.util.concurrent.TimeUnit;

/**
 * Kafka Streams streaming app
 */
public class StreamingApplication implements Runnable {
    // private stream
    // there should never be more than a single stream as the builder should merge them all together
    // so the 'app' stream can be merged with another apps stream for a larger topology
    private KafkaStreams streams;

    public StreamingApplication(DefaultAppConfig config){
        // use this for higher level api
        // can be merge kstreams together and be converted into topology
        //KStreamBuilder builder = new KStreamBuilder();

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
            this.streams = new KafkaStreams(topologyBuilder, config.getKafkaProperties());
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
        // here is where dynamic reloading would take place for this applications stream topology
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
