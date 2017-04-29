package serialization.deserializers;

import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import java.util.Map;

/**
 *  Jackson POJO deserializer
 * @param <T> Type of the class to be deserialized
 */
public class JsonPOJODeserializer<T> implements Deserializer<T> {

    private ObjectMapper objectMapper = new ObjectMapper();

    private Class<T> tClass;

    /**
     * Default constructor needed by Kafka
     */
    public JsonPOJODeserializer() {
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        // get the class name to be deserialized from provided properties
        tClass = (Class<T>) props.get("JsonPOJOClass");
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        // check null
        if (bytes == null) {
            return null;
        }
        // turn the class into type with jackson
        T data;
        try {
            data = objectMapper.readValue(bytes, tClass);
        } catch (Exception e) {
            // give up
            throw new SerializationException(e);
        }
        // give back deserialized version
        return data;
    }

    @Override
    public void close() {
        // cleanup

    }
}