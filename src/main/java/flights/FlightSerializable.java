package flights;

import java.io.Serializable;

public class FlightSerializable implements Serializable {
    private float delayingTime;
    private float maxDelayingTime;
    private int cancelled;
    private int delaying;

    public FlightSerializable(float delayingTime, boolean isCancelled) {
        this.delayingTime += delayingTime;
        if (delayingTime != 0) {
            delaying++;
        }

        if (delayingTime > this.maxDelayingTime) {
            this.maxDelayingTime = delayingTime;
        }

        if (isCancelled) {
            this.cancelled++;
        }
    }
}
