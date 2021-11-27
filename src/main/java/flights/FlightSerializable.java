package flights;

import java.io.Serializable;

public class FlightSerializable implements Serializable {
    private float delayingTime;
    private boolean isCancelled;

    private float maxDelayingTime;
    private int cancelled;
    private int totalDelaying;
    private int totalFlights;

    public FlightSerializable(float delayingTime, boolean isCancelled) {
        this.delayingTime = delayingTime;
        this.isCancelled = isCancelled;
        this.totalFlights++;
        this.maxDelayingTime = delayingTime;

        if (delayingTime != 0) {
            this.totalDelaying++;
        }

        if (isCancelled) {
            this.cancelled++;
        }
    }

    public FlightSerializable AddFlight(FlightSerializable y) {
        this.delayingTime += y.delayingTime;
        this.totalFlights++;

        if (y.delayingTime != 0) {
            this.totalDelaying++;
        }

        if (y.delayingTime > this.maxDelayingTime) {
            this.maxDelayingTime = y.delayingTime;
        }

        if (y.isCancelled) {
            this.cancelled++;
        }

        return this;
    }

    public int getCancelled() {
        return this.cancelled;
    }

    public float getDelayingTime() {
        return delayingTime;
    }

    public float getMaxDelayingTime() {
        return maxDelayingTime;
    }

    public int getTotalDelaying() {
        return totalDelaying;
    }

    public int getTotalFlights() {
        return totalFlights;
    }

    public float getPercentDelaying() {
        return totalDelaying / totalFlights * 100;
    }
}
