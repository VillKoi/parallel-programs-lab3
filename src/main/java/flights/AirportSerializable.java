package flights;

import java.io.Serializable;

public class AirportSerializable implements Serializable {
    private String airportID;
    private String airportName;


    public AirportSerializable(String airportID, String airportName) {
        this.airportID =  airportID;
        this.airportName = airportName;
    }
}
