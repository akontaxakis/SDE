package infore.SDE.reduceFunctions;

import infore.SDE.messages.Estimation;

import java.util.HashMap;

public class CorrelationLSH extends ReduceFunction{

    private HashMap<String,Object> indexEstimations;


    private CorrelationLSH(int nOfP, int count, String[] parameters, int synID, int rqid) {
        super(nOfP,count,parameters,synID,rqid);
    }


    @Override
    public boolean add(Estimation e) {

        indexEstimations.put(e.getKey(),e.getEstimation());
        count++;
        if(count==getnOfP()) {
            return true;
        }
        return false;
    }


    @Override
    public Object reduce() {
        return null;
    }
}
