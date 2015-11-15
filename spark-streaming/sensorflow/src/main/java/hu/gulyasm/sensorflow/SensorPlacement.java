package hu.gulyasm.sensorflow;

import java.io.Serializable;

public class SensorPlacement implements Serializable {
    public final String town;
    public final Person onCallEngineer;

    public SensorPlacement(String town, Person onCallEngineer) {
        this.town = town;
        this.onCallEngineer = onCallEngineer;
    }

    @Override
    public String toString() {
        return "SensorPlacement{" +
                "town='" + town + '\'' +
                ", onCallEngineer=" + onCallEngineer +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SensorPlacement that = (SensorPlacement) o;

        if (!onCallEngineer.equals(that.onCallEngineer)) return false;
        if (!town.equals(that.town)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = town.hashCode();
        result = 31 * result + onCallEngineer.hashCode();
        return result;
    }
}
