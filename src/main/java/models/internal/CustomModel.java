package models.internal;

/**
 * Keep this pluggable to the level that it could be reloaded from a jar at runtime
 */
public class CustomModel {
    public String name;
    public CustomModel(){
        this.name = "nobody";
    }
    public CustomModel(String name){
        this.name = name;
    }

}
