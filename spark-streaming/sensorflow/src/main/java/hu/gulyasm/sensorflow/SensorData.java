package hu.gulyasm.sensorflow;

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;

public class SensorData implements Serializable{

    @SerializedName("SensorID")
    public int sensorID;
    @SerializedName("Type")
    public int type;
    @SerializedName("Value")
    public float value;

    @Override
    public String toString() {
        return "SensorData{" +
                "sensorID=" + sensorID +
                ", type=" + type +
                ", value=" + value +
                '}';
    }


}
