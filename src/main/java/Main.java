import applications.apps.StreamingApplication;
import applications.config.DefaultAppConfig;

public class Main {
// todo:
    // app management
    // proc management
    // async logger
    // abstract/interface
    public static void main(String[]args){
        StreamingApplication app = new StreamingApplication(new DefaultAppConfig());
        app.run();
    }
}
