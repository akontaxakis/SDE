package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import lib.WDFT.controlBucket;
import lib.WDFT.controlPastDFTs;

import java.util.HashMap;


public class PastDFTSynopsis extends Synopsis {

    private controlPastDFTs SynopsisData;
    private String timeIndex;

    public PastDFTSynopsis(int ID, String[] parameters) {
        super(ID, parameters[0], parameters[1]);
        timeIndex = parameters[2];

        SynopsisData = new controlPastDFTs(ID,parameters);
    }


    @Override
    public void add(Object k) {

        JsonNode node = (JsonNode)k;
        String key = node.get(this.keyIndex).asText();
        String value = node.get(this.valueIndex).asText();
        String time = node.get(this.timeIndex).asText();
        SynopsisData.add(time,value,key);

    }

    @Override
    public Object estimate(Object k) {
        return null;
    }

    @Override
    public Estimation estimate(Request rq) {
        HashMap<String, controlBucket> buckets = SynopsisData.estimate(Double.parseDouble(rq.getParam()[0]));

        System.out.println("number of Buckets  -> " + buckets.size());
        System.out.println("threshold  -> " + rq.getParam()[0]);

        return new Estimation(rq, buckets, Integer.toString(rq.getUID()));
    }

    @Override
    public Synopsis merge(Synopsis sk) {
        return null;
    }

}
