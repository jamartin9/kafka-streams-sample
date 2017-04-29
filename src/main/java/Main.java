import application.StreamingApplication;

public class Main {

    public static void main(String[]args){
        StreamingApplication app = new StreamingApplication("my-app", "localhost:9092");
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        app.run();
    }
}
