package flights;

import java.io.Serializable;

public class FlightSerializable implements Serializable {
    private float delayingTime;
    private int cancelled;

    public FlightSerializable(float delayingTime, boolean isCancelled) {
        this.delayingTime = delayingTime;
        if (isCancelled) {
            this.cancelled++;
        }
    }
}
