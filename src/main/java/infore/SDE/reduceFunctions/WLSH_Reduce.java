package infore.SDE.reduceFunctions;

import infore.SDE.reduceFunctions.ReduceFunction;
import infore.SDE.messages.Estimation;
import lib.WLSH.Bucket;
import lib.WLSH.SimilarSims;
import lib.WLSH.WLSH;

import java.util.*;

public class WLSH_Reduce extends ReduceFunction {

    private Bucket bt;
    private final int numOfP;
    private int count;
    private final int K;
    double threshold;
    int queryId;
    private String Key;
    private HashMap<Integer, SimilarSims[]> TopKPerBucket ;
    List<SimilarSims> kList;
    int k;
    String list_of_sims;
    int T;

    public WLSH_Reduce(int numOfP, int count, String y, double th, int k,int q_id, String sims,int num_of_times) {
        K = k;
        threshold = th;
        Key = y;
        queryId = q_id;
        list_of_sims = sims;
        bt = new Bucket(th, y, 10);
        this.numOfP = numOfP;
        this.count = count;
        TopKPerBucket = new HashMap<>();
        kList = new ArrayList<>();
        T = num_of_times;
    }

    public WLSH_Reduce(int noOfP, int count, String estimationkey, double th, int k) {
        K = k;
        threshold = th;
        Key = estimationkey;
        this.numOfP = noOfP;
        bt = new Bucket(th, Key, 10);
        this.count = count;
        TopKPerBucket = new HashMap<>();
        kList = new ArrayList<>();
    }

    @Override
    public boolean add(Estimation e) {
        Bucket t_bt = (Bucket)e.getEstimation();
        String key = e.getEstimationkey();
        bt.add(t_bt);
        count++;
        if (count == numOfP) {
            bt.toString();
            return true;
        }
        return false;
    }
    @Override
    public Object reduce() {
        String[] splited = list_of_sims.split("&");
        ArrayList<String> list = new ArrayList<>();
        Collections.addAll(list, splited);
        System.out.println(" ");
        System.out.println("Start (Reduce) for bucket ->" + bt.toString());
        HashMap<String,ArrayList<SimilarSims>> SS = new HashMap<>();
        if (queryId == 1) {
            SS = bt.compareForSimilarity(K, threshold);
        }
        else if(queryId == 2){
            String mysim = list.get(0);
            String[] s1 = mysim.split("\\_");
            String simulation = s1[0];
            String start_time = s1[1];
            SS = bt.produceTopKForOneSim(K,threshold,simulation,Integer.parseInt(start_time),T);
        }
        else if(queryId == 3){
            SS = bt.produceTopKPerSim(K,threshold,list,T);
        }
        else{
            System.out.println("You have not specify query type");
        }

        for (Map.Entry<String, ArrayList<SimilarSims>> set : SS.entrySet()) {
            System.out.print("Simulation:" +set.getKey()+ "|-> ");
            for(SimilarSims s:set.getValue()){
                System.out.println(s.toString()+" ");
            }
        }
        System.out.println("Finished (Reduce) for bucket ->" + bt.toString());

        return SS;
    }
}
