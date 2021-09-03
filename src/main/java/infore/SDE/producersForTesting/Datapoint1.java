package infore.SDE.producersForTesting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;


public class Datapoint1 implements Serializable {

    private static final long serialVersionUID = 1L;

    private String dataSetkey;
    private String streamID;//hash value
    private JsonNode values;


    public String getstreamID() {
        return streamID;
    }

    public void setstreamID(String streamId) {
        streamID = streamId;
    }


    public Datapoint1(String key, JsonNode mp) {
        this.dataSetkey = key;
        this.values = mp;
    }

    public Datapoint1() {

    }

    public Datapoint1(String bio_useCase, String simulation, JsonNode node) {
        this.dataSetkey = bio_useCase;
        this.values = node;
        this.streamID = simulation;
    }

    public String ValueToKafka() {
        return "\""+dataSetkey+","+values+"\"";
    }

    public String keyToKafka() {
        //int randomValue = 10 + (100 - 10) * r.nextInt((100 - 10) + 1) + 10;
        return "\""+dataSetkey+"\"";
    }

    public String getDataSetkey() {
        return dataSetkey;
    }

    public void setDataSetkey(String key) {
        this.dataSetkey = key;
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
                "DataSetkey='" + dataSetkey + '\'' +
                ", values='" + values + '\'' +
                '}';
    }

}
