package hu.gulyasm.sensorflow;

import java.io.Serializable;
import java.util.HashMap;

public class SensorDB implements Serializable {

    public HashMap<Integer, SensorPlacement> data;

    public SensorDB() {
        data = new HashMap<>();
        data.put(0, new SensorPlacement("Budapest", new Person("Mate Gulyas", "hello@example.com")));
        data.put(1, new SensorPlacement("New York", new Person("John Doe", "doe@example.com")));
    }

    public SensorPlacement getSensorPlacement(int sensorID) {
        return data.get(sensorID);
    }
}
