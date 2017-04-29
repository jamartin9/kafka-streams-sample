package serialization.utils;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for managing serializers. Can be used to minimize instances of repeated serializers
 */
public class Utility {

    /**
     *
     * @param type class to use for serde
     * @param <T> type of the class
     * @return returns serde conversion
     */
    public static <T> Serde<T> getSerde(Class<T> type){
        return Serdes.serdeFrom(type);
    }

    /**
     * Helper method for Serde with custon serializers and deserializers
     *
     * @param desiredType class to create the serializers for
     * @param deserializer deserializer class. must extend kafka deserializer
     * @param serializer serializer class. must extend kafka serializer
     * @param <T> the type to use
     * @return returns serde conversion of the desiredType for serializers/deserialzers
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <T> Serde<T> getSerde(Class<T> desiredType, Class <? extends Deserializer> deserializer, Class<? extends Serializer> serializer) throws IllegalAccessException, InstantiationException {
        Map<String, Object> serdeProps = new HashMap<>();
        final Deserializer<T> modelDeserializer = deserializer.newInstance();
        // all de/serializers should look for the property POJOClass to find their corresponding POJO
        serdeProps.put("POJOClass", desiredType);
        modelDeserializer.configure(serdeProps, false);
        final Serializer<T> modelSerializer = serializer.newInstance();
        serdeProps.put("POJOClass", desiredType);
        modelSerializer.configure(serdeProps, false);
        return Serdes.serdeFrom(modelSerializer, modelDeserializer);
    }

    /**
     * Helper method for deserializers
     *
     * @param desiredType class to create a deserializer for
     * @param deserializer class to use as a deserializer. must extend kafka serializer
     * @param <T> type of the class
     * @return returns a deserializer for the provided class
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <T> Deserializer<T> getDeserializer(Class<T> desiredType, Class <? extends Deserializer> deserializer) throws IllegalAccessException, InstantiationException {
        Map<String, Object> serdeProps = new HashMap<>();
        final Deserializer<T> modelDeserializer = deserializer.newInstance();
        // all de/serializers should look for the property POJOClass to find their corresponding POJO
        serdeProps.put("POJOClass", desiredType);
        modelDeserializer.configure(serdeProps, false);
        return modelDeserializer;
    }

    /**
     * Serialization helper method
     *
     * @param desiredType The class to create a serializer for
     * @param serializer The class to use as a serializer. must extend kafka serializer
     * @param <T> type of class to return serializer for
     * @return returns serializer created for the provided classes
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <T> Serializer<T> getSerializer(Class<T> desiredType, Class<? extends Serializer> serializer) throws IllegalAccessException, InstantiationException {
        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<T> modelSerializer = serializer.newInstance();
        serdeProps.put("POJOClass", desiredType);
        modelSerializer.configure(serdeProps, false);
        return modelSerializer;
    }

    /**
     * String serializer helper method
     * @return returns String serializer
     */
    public static StringSerializer getStringSerializer(){
        return new StringSerializer();
    }

    /**
     * String deserializer helper method
     * @return returns String deserializer
     */
    public static StringDeserializer getStringDeserializer(){
        return new StringDeserializer();
    }

    /**
     * Json serializer helper method
     * @return returns kafka json connect serializer
     */
    public static Serializer<JsonNode> getJSONSerializer(){
        return new JsonSerializer();
    }

    /**
     * Json deserializer helper method
     * @return returns Kafka Json Connect deserializer
     */
    public static Deserializer<JsonNode> getJSONDeserializer(){
        return new JsonDeserializer();
    }

    /**
     * Json helper method
     * @return returns the serde json from kafka-json-connect
     */
    public static Serde<JsonNode> getJSONSerde(){
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
