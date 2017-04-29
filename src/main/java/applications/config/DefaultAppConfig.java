package applications.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.UUID;

/**
 * Default Kafka Stream App Config
 */
public class DefaultAppConfig {
    // properties for kafka config
    private Properties kafkaProps;

    /**
     * Default config options
     */
    public DefaultAppConfig(){
        this.kafkaProps = new Properties();
        // stream specific config
        this.kafkaProps.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID());
        this.kafkaProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("zookeeper.host","localhost:9092"));
        this.kafkaProps.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        this.kafkaProps.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // consumer config settings
        this.kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    /**
     * retrieves the kafka properties
     * @return returns properties to be used for kafka
     */
    public Properties getKafkaProperties(){
        return this.kafkaProps;
    }

    /**
     * Sets a property for kafka
     * @param key The key for the property to be set
     * @param value The value to the the key to
     */
    public void setKafkaProperty(String key, String value){
        this.kafkaProps.put(key, value);
    }
}
