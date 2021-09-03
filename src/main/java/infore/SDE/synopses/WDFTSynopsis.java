package infore.SDE.synopses;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

public class WDFTSynopsis extends Synopsis {


    public WDFTSynopsis(int ID, String k, String v) {
        super(ID, k, v);
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
