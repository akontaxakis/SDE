package infore.SDE.producersForTesting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;


public class Datapoint implements Serializable {

    private static final long serialVersionUID = 1L;

    private String DataSetkey; //hash value
    private String StreamID; //the stream ID
    private JsonNode values;

    public Datapoint(String key, String streamID, JsonNode mp) {
        this.DataSetkey = key;
        this.StreamID = streamID;
        this.values = mp;
    }

    public Datapoint() {

    }

    public Datapoint(String simulation, JsonNode node) {
    }

    public String ValueToKafka() {
        return "\""+DataSetkey+","+StreamID+","+values+"\"";
    }

    public String keyToKafka() {
        //int randomValue = 10 + (100 - 10) * r.nextInt((100 - 10) + 1) + 10;
        return "\""+DataSetkey+"\"";
    }

    public String getDataSetkey() {
        return DataSetkey;
    }

    public void setDataSetkey(String key) {
        this.DataSetkey = key;
    }

    public String getStreamID() {
        return StreamID;
    }

    public void setStreamID(String streamID) {
        StreamID = streamID;
    }

    public JsonNode getValues() {
        return values;
    }

    public void setValues(JsonNode value) {
        this.values = value;
    }

    public String toJsonString() throws JsonProcessingException {
        return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
    }


    @Override
    public String toString() {
        return "Datapoint{" +
                "DataSetkey='" + DataSetkey + '\'' +
                ", StreamID='" + StreamID + '\'' +
                ", values='" + values + '\'' +
                '}';
    }

}
