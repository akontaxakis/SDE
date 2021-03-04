package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.simulations.WLSH.WLSHsynopsis;

import java.util.ArrayList;
import java.util.HashMap;

public class WLSHSynopses extends Synopsis {

    private WLSHsynopsis wlsh;
    private String timeIndex;

    public WLSHSynopses(int ID, String[] parameters) {

        super(ID, parameters[0],parameters[1]);
        timeIndex = parameters[2];
        wlsh = new WLSHsynopsis(Integer.parseInt(parameters[3]), Integer.parseInt(parameters[4]));

    }

    @Override
    public void add(Object k) {

        JsonNode node = (JsonNode)k;
        String key = node.get(this.keyIndex).asText();
        String value = node.get(this.valueIndex).asText();
        int time = Integer.parseInt(node.get(this.timeIndex).asText());
        String[] tokens = value.split(",");

        wlsh.add(key,time, StringArrayToInterArray(tokens));

    }

    @Override
    public Object estimate(Object k) {
        return null;
    }

    @Override
    public Estimation estimate(Request rq) {
        HashMap<Integer, ArrayList<String>> buckets = wlsh.estimate(Double.parseDouble(rq.getParam()[0]));

        System.out.println("number of Buckets  -> " + buckets.size());
        System.out.println("threshold  -> " + rq.getParam()[0]);

        return new Estimation(rq, buckets, Integer.toString(rq.getUID()));
    }

    @Override
    public Synopsis merge(Synopsis sk) {
        return null;
    }

    private int[] StringArrayToInterArray(String[] StringArray){
        int[] A = new int[StringArray.length];
            for(int i = 0;i<StringArray.length; i++){
                A[i] = Integer.parseInt(StringArray[i]);

            }
        return A;
    }
}
