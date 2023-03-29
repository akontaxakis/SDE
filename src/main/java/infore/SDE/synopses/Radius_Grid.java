package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Datapoint;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.util.ArrayList;
import java.util.HashMap;

public class Radius_Grid extends ContinuousSynopsis{

    //GRID_ID //CELL, LIST OF STREAMS
    private HashMap<Integer, ArrayList<String>> grid = new HashMap<>();
    private Request rq;
    public Radius_Grid(int ID, String k, String v) {
        super(ID, k, v);
    }

    public Radius_Grid(Request rq) {
        super(rq.getUID(), rq.getParam()[0], rq.getParam()[1], rq.getParam()[2]);
        this.rq = rq;
    }

    @Override
    public Estimation addEstimate(Object k) {

        Datapoint dp = (Datapoint) k;
        JsonNode node = dp.getValues();
        String key = dp.getStreamID();
        String values = node.get("value").asText();
        String[] prices = values.split(";");

        Estimation e;

        int cell = Integer.parseInt(prices[0])*100000 + Integer.parseInt(prices[1]);
        if(grid == null){
            grid  = new HashMap<>();
            ArrayList<String> dps = new ArrayList<>();
            e = new Estimation(rq, key, dps);
            dps.add(dp.getStreamID());
            grid.put(cell,dps);

            return e;
        }else{
            if(grid.containsKey(cell)){
                ArrayList<String> dps = grid.get(cell);
                e = new Estimation(rq, key, dps);
                dps.add(dp.getStreamID());
                grid.put(cell,dps);

            }else{
                ArrayList<String> dps = new ArrayList<>();
                e = new Estimation(rq, key, dps);
                dps.add(dp.getStreamID());
                grid.put(cell,dps);
            }
        }
        return e;
    }


    public ArrayList<Estimation> add_and_provide_estimates(Object k) {
        ArrayList<Estimation> est = new ArrayList<>();
        Datapoint dp = (Datapoint) k;
        JsonNode node = dp.getValues();
        String key = dp.getStreamID();
        String values = node.get("value").asText();
        String[] prices = values.split(";");

        int cell = Integer.parseInt(prices[0])*100000 + Integer.parseInt(prices[1]);
        if(grid == null){
            grid  = new HashMap<>();
            ArrayList<String> dps = new ArrayList<>();
            dps.add(dp.getStreamID());
            grid.put(cell,dps);
            return null;
        }else{
            if(grid.containsKey(cell)){
                ArrayList<String> dps = grid.get(cell);
                dps.add(dp.getStreamID());
                grid.put(cell,dps);
            }else{
                ArrayList<String> dps = new ArrayList<>();
                dps.add(dp.getStreamID());
                grid.put(cell,dps);
            }
        }





        return est;
    }


    @Override
    public void add(Object k) {

    }

    @Override
    public Object estimate(Object k) {
        return null;
    }

    @Override
    public Estimation estimate(Request rq) {
        return null;
    }

    @Override
    public Synopsis merge(Synopsis sk) {
        return null;
    }
}
