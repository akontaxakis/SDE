package infore.SDE.reduceFunctions;

import infore.SDE.messages.Estimation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RadiusReduce extends ReduceFunction {


    private HashMap<String,Integer> counts;
    private String results;
    private int threshold;
    public RadiusReduce(int nOfP, int count, String[] parameters, int syn, int rq) {
        super(nOfP, count, parameters, syn, rq);
        counts = new HashMap<>();
        results = "";
        threshold = nOfP*Integer.parseInt(parameters[0])/100;
    }

    @Override
    public boolean add(Estimation e) {
        String partial_results = (String)e.getEstimation();
        //System.out.println(partial_results);
        String[] streams = partial_results.split(";");
        for(int i=0;i< streams.length;i++){
            if(counts.containsKey(streams[i])) {
                int partial_sum = counts.get(streams[i]);
                partial_sum++;
                counts.put(streams[i],partial_sum);
            }else{
                counts.put(streams[i],1);
            }
        }
        count++;
        if (count == nOfP) {
            return true;
        }
        return false;
    }


    @Override
    public Object reduce() {
        for(Map.Entry<String,Integer> stream:counts.entrySet()){
            if(threshold<stream.getValue()){
                results=results+";"+stream.getKey();
            }
        }
        System.out.println(results);
        return results;
    }


}