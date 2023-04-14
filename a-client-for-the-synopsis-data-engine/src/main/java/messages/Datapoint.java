package infore.SDE.messages;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import java.io.IOException;
import java.io.Serializable;


public class Datapoint implements Serializable {

    private static final long serialVersionUID = 1L;
    private String DataSetkey; //hash value
    private String StreamID; //the stream ID
    private JsonNode values;

    public Datapoint(String key, String streamID, JsonNode value) {
        this.DataSetkey = key;
        StreamID = streamID;
        this.values = value;

    }
    public Datapoint() {

    }

    public Datapoint(String d, String s, Object o) {
        DataSetkey=d; //hash value
        StreamID=s; //the stream ID
        String jsonString = "{\"value\":\"" +o+ "\"}";
        ObjectMapper mapper = new ObjectMapper();
        try {
            values = mapper.readTree(jsonString);
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    public String  getKey() {
        return this.DataSetkey;
    }


    public boolean compare(Datapoint dp) {

        JsonNode a1 = dp.getValues();
        String values = a1.get("price").asText();
        String[] a1_prices = values.split(";");

        values = this.values.get("price").asText();
        String[] b1_prices = values.split(";");



        double[] x = new double[a1_prices.length];
        double[] y = new double[a1_prices.length];
        for(int i=0;i<a1_prices.length;i++){
            x[i] = Double.parseDouble(a1_prices[i]);
            y[i] = Double.parseDouble(b1_prices[i]);
        }

        double corr = new PearsonsCorrelation().correlation(y, x);
        if(corr> 0.7) {
            //System.out.println(corr);
            return true;
        }else{
            return false;
        }
    }
}
