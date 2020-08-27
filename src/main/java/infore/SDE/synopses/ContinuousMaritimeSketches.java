package infore.SDE.synopses;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.io.IOException;
import java.util.HashMap;


public class ContinuousMaritimeSketches extends ContinuousSynopsis{
    private HashMap<String, MaritimeSketch> Synopses;
    String[] parameters;

    public ContinuousMaritimeSketches(int uid,Request rq, String[] param) {
        super(uid, param[0], param[1]);
        Synopses = new HashMap<String, MaritimeSketch>();
        parameters = param;
        this.setRq(rq);
    }

    @Override
    public void add(Object k) {
        String j = (String)k;

        String Dataset = j.substring(0, j.indexOf(','));
        String rest = j.substring(j.indexOf(' ') + 1);
        String StreamId = rest.substring(0, j.indexOf(','));
        String Data = rest.substring(j.indexOf(' ') + 1);

        MaritimeSketch mTs = Synopses.get(StreamId);
        if(mTs == null)
            mTs = new MaritimeSketch(this.SynopsisID,parameters);

        mTs.add(Data);
        Synopses.put(StreamId, mTs);

    }
    @Override
    public Estimation addEstimate(Object k) {
        String j = (String)k;
        ObjectMapper jackson_mapper = new ObjectMapper();
        ObjectNode curr = null;
        try {
            curr = (ObjectNode) jackson_mapper.readTree(j);
        } catch (IOException e) {
            e.printStackTrace();
        }

        MaritimeSketch mTs = Synopses.get(curr.get("ship").asText());
        if(mTs == null)
            mTs = new MaritimeSketch(this.SynopsisID,parameters);

        String Estimation = mTs.addEstimate(k);
        System.out.println(Estimation);
        Synopses.put(curr.get("ship").asText(), mTs);

        return new Estimation(this.rq, Estimation, Integer.toString(rq.getUID()));

    }

    @Override
    public Object estimate(Object k) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Synopsis merge(Synopsis sk) {
        // TODO Auto-generated method stub
        return null;
    }

    public Estimation estimate(Request rq) {

      return null;
    }




}
