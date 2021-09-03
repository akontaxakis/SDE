package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
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
        super(uid, param[0], param[1], param[2]);
        Synopses = new HashMap<>();
        parameters = param;
        this.setRq(rq);
    }

    @Override
    public void add(Object k) {
        //String j = (String)k;
        JsonNode node = (JsonNode)k;
        String StreamId = node.get(this.keyIndex).asText();

        MaritimeSketch mTs = Synopses.get(StreamId);
        if(mTs == null)
            mTs = new MaritimeSketch(this.SynopsisID,parameters);

        mTs.add(node);
        Synopses.put(StreamId, mTs);

    }
    @Override
    public Estimation addEstimate(Object k) {
        JsonNode node = (JsonNode)k;
        ObjectMapper jackson_mapper = new ObjectMapper();
        ObjectNode curr = null;

        try {
            curr = (ObjectNode) jackson_mapper.readTree(node.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(curr!=null) {
            MaritimeSketch mTs = Synopses.get(curr.get("shipid").asText());
            if (mTs == null)
                mTs = new MaritimeSketch(this.SynopsisID, parameters);

            String Estimation = mTs.addEstimate(node.toString());
            //System.out.println(Estimation);
            Synopses.put(curr.get("shipid").asText(), mTs);

            return new Estimation(this.rq, Estimation, Integer.toString(rq.getUID()));
        }

        return new Estimation(this.rq, null, Integer.toString(rq.getUID()));
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
