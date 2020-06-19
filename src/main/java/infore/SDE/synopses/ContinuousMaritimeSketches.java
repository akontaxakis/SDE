package infore.SDE.synopses;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.sketches.TimeSeries.COEF;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

        String Dataset = j.substring(0, j.indexOf(','));
       // System.out.println("dataset " + Dataset);
        String rest = j.substring(j.indexOf(',') + 1);
        String StreamId = rest.substring(0, rest.indexOf('{')-1);
        String Data = rest.substring(rest.indexOf('{'));
       // System.out.println("StreamId " + StreamId);
       // System.out.println("Data " + Data);
        MaritimeSketch mTs = Synopses.get(StreamId);
        if(mTs == null)
            mTs = new MaritimeSketch(this.SynopsisID,parameters);

        String Estimation = mTs.addEstimate(Data);
        Synopses.put(StreamId, mTs);

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
