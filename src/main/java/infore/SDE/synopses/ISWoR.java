package infore.SDE.synopses;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import java.util.BitSet;
import java.util.Random;

public class ISWoR extends ContinuousSynopsis{

    private int p;
    private Random rando;
    private Random randoInt;

    public ISWoR(int ID, Request t_rq, String[] params) {
        super(ID, params[0], params[1],params[2]);
        p=1;
        rando= new Random();
        randoInt = new Random();
        this.setRq(t_rq);
    }

    @Override
    public Estimation addEstimate(Object k) {

        BitSet bts = new BitSet(p+1);
        int seed = randoInt.nextInt();
        rando.setSeed(seed);

        for(int i = 0; i<p;i++) {
            bts.set(i, rando.nextBoolean());
        }
        for(int i = 0; i<p;i++) {
            if (bts.get(i)) return null;
        }
        return new Estimation(rq,k,Integer.toString(rq.getUID()));
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