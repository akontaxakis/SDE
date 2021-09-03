package lib.TopK;

import java.util.HashMap;
import java.util.Map;

public class TopK {

    //the number of top K
   private int k;
    //the countdown
    private int  cd;
    private int worstTime;
    private int worstAlive;
    private String worstId;
    private HashMap<String, Simulation> Simulations = new HashMap<>();
    private HashMap<String, Simulation>  topK = new HashMap();


    public TopK(int parseInt, int parseInt1) {
        k = parseInt;
        cd = parseInt1;
    }

    public void add(String key,String values,int time) {

        //String[] tokens = values.split(",");
        //int n_alive = Integer.parseInt(tokens[0]);
        int alive = Integer.parseInt(values);

        Simulation curr;
        boolean rv,newTopK;
        newTopK = false;
        rv = false;
        if(Simulations.get(key)==null){
            curr = new Simulation(cd, key,alive,time,false);
            Simulations.put(curr.getPid(),curr);
            if(topK.size()<k){
                curr.setTopk(true);
                topK.put(curr.getPid(),curr);
                if(topK.size()==k)
                    findWorst();
            }
            Simulations.put(curr.getPid(),curr);

        }else{
            curr = Simulations.get(key);
            curr.updateData(time, alive);
            Simulations.put(curr.getPid(),curr);
            if(curr.isTopk()){
                topK.put(curr.getPid(),curr);
                if(topK.size()==k){
                        if(worstId==null)
                            findWorst();
                        else {
                            if (worstId.equals(key)) {
                                findWorst();
                            }
                        }
                }
            }else{
                if(worstTime == time){
                    if(curr.getAlive() < worstAlive)
                        newTopK = true;
                    else{
                        if((curr.getAlive() - worstAlive) > curr.getDowntrend()){
                            curr.setCountDown(curr.getCountDown() - 1);
                            curr.setDowntrend(curr.getAlive() - worstAlive);
                            if((curr.getCountDown() ==0)){
                                rv = true;
                            }
                        }
                    }
                }else if (worstTime > time){
                    if(curr.getAlive() < topK.get(worstId).getAlive(time))
                        newTopK = true;
                    else{
                        if((curr.getAlive() - topK.get(worstId).getAlive(time)) > curr.getDowntrend()){
                            curr.setCountDown(curr.getCountDown() - 1);
                            curr.setDowntrend(curr.getAlive() - topK.get(worstId).getAlive(time));
                            if((curr.getCountDown() ==0)){
                                rv = true;
                            }
                        }
                    }
                }else{
                    if(curr.getAlive(worstTime) < worstAlive)
                        newTopK = true;
                    else{
                        if((curr.getAlive(worstTime) - worstAlive) > curr.getDowntrend()){
                            curr.setCountDown(curr.getCountDown() - 1);
                            curr.setDowntrend(curr.getAlive(worstTime) - worstAlive);
                            if((curr.getCountDown() ==0)){
                                rv = true;
                            }
                        }
                    }
                }


                if(newTopK){
                    curr.setTopk(true);
                    topK.remove(worstId);
                    topK.put(curr.getPid(),curr);
                    findWorst();
                }
                if(rv){
                    Simulations.remove(curr.getPid());
                    topK.remove(curr.getPid());
                }

            }
        }
    }




    private void findWorst(){
        int time =1000000000;
        for (Map.Entry<String, Simulation> entry : topK.entrySet()) {
            if (entry.getValue().getCurrtime() < time) {
                time = entry.getValue().getCurrtime();
            }
        }
        int i =0;
        int worst = 0;
        String w = null;
        for (Map.Entry<String, Simulation> entry : topK.entrySet()) {
            if (i == 0) {
                worst = entry.getValue().getAlive(time);
                w = entry.getValue().getPid();
                i++;
            } else {
                if (entry.getValue().getAlive(time) > worst) {
                    worst = entry.getValue().getAlive(time);
                    w = entry.getValue().getPid();
                }
            }
        }
        setWorstAlive(worst);
        setWorstId(w);
        setWorstTime(time);
    }



    public Object estimate() {

        System.out.println("ESTIMATIONS -> " + topK.size());
        return topK;
    }


    public int getWorstTime() {
        return worstTime;
    }

    public void setWorstTime(int worstTime) {
        this.worstTime = worstTime;
    }

    public int getWorstAlive() {
        return worstAlive;
    }

    public void setWorstAlive(int worstAlive) {
        this.worstAlive = worstAlive;
    }

    public String getWorstId() {
        return worstId;
    }

    public void setWorstId(String worstId) {
        this.worstId = worstId;
    }



}
