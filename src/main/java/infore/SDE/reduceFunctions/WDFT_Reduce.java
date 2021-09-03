package infore.SDE.reduceFunctions;

import infore.SDE.messages.Estimation;
import lib.WDFT.controlBucket;
import lib.WDFT.correlationPastDFT;

public class WDFT_Reduce extends ReduceFunction {

    private correlationPastDFT bt;

    public WDFT_Reduce(int workers, double th, int k, int t, String stockN, int numQ, String key) {
       bt = new correlationPastDFT(workers,  th,  k,  t,  stockN, numQ, key);
    }

    public WDFT_Reduce(int noOfP, double th, int k, int t, String[] stringToStringArray) {
        bt = new correlationPastDFT(noOfP, th,k,t,stringToStringArray[0],Integer.parseInt(stringToStringArray[0]),stringToStringArray[0]);
    }

    @Override
    public boolean add(Estimation e) {
       return bt.merge((controlBucket)e.getEstimation());
    }

    @Override
    public Object reduce() {
        return bt.reduce();
    }
}
