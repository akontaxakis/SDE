package infore.SDE.ReduceFunctions;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.Random;

public class CoordinatorFunction  {

    private int s;
    private int p;
    private ArrayList<Object> tj;
    private ArrayList<Object> tj_1;
    private ArrayList<Integer> seeds;
    private Random rando;

    public CoordinatorFunction(int windowSize){
        s = windowSize;
        rando = new Random();
        p=0;
        tj= new ArrayList<>();
        tj_1=  new ArrayList<>();
        seeds = new ArrayList<>();

    }


public Request add(Estimation e){
        int seed= Integer.parseInt(e.getEstimationkey());
        BitSet bts = new BitSet(p+1);
        Object value = e.getEstimation();
        rando.setSeed(seed);
        for(int i = 0; i<p+1;i++) {
            bts.set(i, rando.nextBoolean());
        }
        if(bts.get(p+1)){
            if(tj.size()<s) tj.add(value);
        }else{
            tj_1.add(value);
            seeds.add(seed);
        }
        if(tj_1.size() == s){
            ArrayList<Object> temp = new ArrayList<>();
            ArrayList<Object> temp_1 = new ArrayList<>();
            ArrayList<Integer> temp_s = new ArrayList<>();
            p = p+1;
            for(int g=0;g<s;g++) {

                rando.setSeed(seeds.get(g));
                for (int i = 0; i < p + 1; i++) {
                    bts.set(i, rando.nextBoolean());
                }
                if(bts.get(p+1)){
                    temp.add(tj_1.get(g));
                }else{
                    temp_1.add(tj_1.get(g));
                    temp_s.add(seeds.get(g));
                }
            }
            tj=temp;
            tj_1=temp_1;
            seeds=temp_s;

            return new Request();
        }

        return null;


    }
}