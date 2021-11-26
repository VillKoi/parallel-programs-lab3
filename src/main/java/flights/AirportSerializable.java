package flights;

import java.io.Serializable;

public class AirportSerializable implements Serializable {
    private Integer airportID;
    private String airportName;


    public AirportSerializable(Integer airportID, String airportName) {
        this.airportID =  airportID;
        this.airportName = airportName;
    }
}
