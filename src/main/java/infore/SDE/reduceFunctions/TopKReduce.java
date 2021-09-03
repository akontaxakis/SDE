package infore.SDE.reduceFunctions;

import infore.SDE.messages.Estimation;
import lib.TopK.Simulation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class TopKReduce extends ReduceFunction {

    private HashMap<String, Simulation> topK;
    public TopKReduce(int nOfP, int count, String[] parameters, int Syn, int rq) {
        super(nOfP, count, parameters,Syn, rq);
        topK = new HashMap();
    }


    @Override
    public boolean add(Estimation e) {

        topK.putAll((HashMap<String, Simulation>)e.getEstimation());
        count++;
        if(count == nOfP) {
            return true;
        }
        return false;
    }

    @Override
    public Object reduce() {
        String TopK= "|";
        ArrayList<Integer> t_TopK= new ArrayList<>();
        int time =Integer.parseInt(this.parameters[1]);
            for (Simulation value : topK.values()) {
               t_TopK.add(value.getAlive(time));
            }
        Collections.sort(t_TopK);
        int finalK = t_TopK.get(Integer.parseInt(this.parameters[0]));
        for (Simulation value : topK.values()) {
            if (finalK>value.getAlive(time)){
                TopK = TopK+"|"+value.getPid();
        }
    }
        System.out.println("FINAL K -> "+TopK);
            return TopK;
}

}
