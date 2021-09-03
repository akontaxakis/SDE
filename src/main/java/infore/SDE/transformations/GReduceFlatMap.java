package infore.SDE.transformations;

import infore.SDE.messages.Estimation;
import lib.WDFT.PAIR;
import lib.WLSH.SimilarSims;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class GReduceFlatMap extends RichFlatMapFunction<Estimation, Estimation> {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private HashMap<String, Object> state = new HashMap<>();


    @Override
    public void flatMap(Estimation value, Collector<Estimation> out) {
       // System.out.println(value.toString());
        int id = value.getSynopsisID();
        boolean init = false;
        Object tmp = state.get(value.getEstimationkey());
        if (tmp == null){
           init = true;
        }
        /*if (id == 28) {

            SimilarSims[] SS =(SimilarSims[])value.getEstimation();
            HashMap<String,ArrayList<SimilarSims>> SS1 = (HashMap<String, ArrayList<SimilarSims>>) value.getEstimation();
            ArrayList<SimilarSims> kList;
            if(init) {
                kList = new ArrayList<>();
            }
            else{
                kList =  (ArrayList<SimilarSims>)tmp;
            }
            for(SimilarSims ss1: SS){
                kList.add(ss1);
            }

            List<SimilarSims> finalTopK = produceFinalTopK(kList, Integer.parseInt(value.getParam()[1]));
            value.setEstimation(SimilarSimsToString(finalTopK));
            state.put(value.getEstimationkey(),kList);
        }*/
       // if (id == 29) {
       //     List<PAIR> pairs = (List<PAIR>)value.getEstimation();
//            ArrayList<PAIR> kList;
//            if (init) {
//                kList = new ArrayList<>();
//            }
//            else{
//                kList = (ArrayList<PAIR>)tmp;
//            }
//            for (PAIR p: pairs) {
//                kList.add(p);
//            }
//
//            List<PAIR> finalTopK = produceTopKStocks(kList, Integer.parseInt(value.getParam()[1]));
//            value.setEstimation(SimilarStocksToString(finalTopK));
//            state.put(value.getEstimationkey(),kList);
       // }
        out.collect(value);
    }
    private List<SimilarSims> produceFinalTopK( List<SimilarSims> kList, int K ){
        List<SimilarSims> topkList = new ArrayList<>();

        System.out.println("After add into list -> " + kList.size());
        for(SimilarSims val1: kList){
            System.out.println(val1.getCorrelation());
        }
        kList.sort(Collections.reverseOrder());

        if(kList.size()>K){
            for(int i = 0; i<K; i++){
                topkList.add(kList.get(i));
            }
        }
        else{
            return kList;
        }

        return topkList;
    }

    private List<PAIR> produceTopKStocks(List<PAIR> kList, int K) {
        List<PAIR> topk = new ArrayList<>();
        System.out.println("After add into list -> " + kList.size());
        for(PAIR val1: kList){
            System.out.println(val1.getCorrelation());
        }
        kList.sort(Collections.reverseOrder());
        if(kList.size()>K){
            for(int i = 0; i<K; i++){
                topk.add(kList.get(i));
            }
        }
        else{
            return kList;
        }
        return topk;
    }

    private String SimilarSimsToString(List<SimilarSims> finalTopK){
        String tmp = "";
        for(SimilarSims val1: finalTopK){
            tmp = tmp  + val1.getCorrelation() +"_";
        }
        System.out.println("output -> " + tmp);
        return tmp;
    }

    private String SimilarStocksToString(List<PAIR> finalTopK){
        String tmp = "";
        for(PAIR val1: finalTopK){
            tmp = tmp  + val1.getCorrelation() +"_";
        }
        System.out.println("output -> " + tmp);
        return tmp;
    }

}
