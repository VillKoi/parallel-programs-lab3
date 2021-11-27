package flights;

import java.io.Serializable;

public class FlightSerializable implements Serializable {
    private float delayingTime;
    private boolean isCancelled;

    private float maxDelayingTime;
    private int cancelled;
    private int delaying;
    private int totalFlights;

    public FlightSerializable(float delayingTime, boolean isCancelled) {
        this.delayingTime = delayingTime;
        this.isCancelled = isCancelled;
        this.totalFlights++;
        this.maxDelayingTime = delayingTime;

        if (delayingTime != 0) {
            this.delaying++;
        }

        if (isCancelled) {
            this.cancelled++;
        }
    }

    public FlightSerializable AddFlight(FlightSerializable y) {
        this.delayingTime += y.delayingTime;
        this.totalFlights++;

        if (y.delayingTime != 0) {
            this.delaying++;
        }

        if (y.delayingTime > this.maxDelayingTime) {
            this.maxDelayingTime = y.delayingTime;
        }

        if (y.isCancelled) {
            this.cancelled++;
        }

        return this;
    }
}
