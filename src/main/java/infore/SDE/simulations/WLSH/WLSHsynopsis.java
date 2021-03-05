package infore.SDE.simulations.WLSH;

import java.util.*;

import infore.SDE.simulations.WLSH.WLSH;
import org.apache.commons.math3.distribution.NormalDistribution;

public class WLSHsynopsis {

    private HashMap<String, TreeMap<Integer, WLSH>> externalHashMap;
    private HashMap<Integer, ArrayList<String>> Buckets;
    private HashMap<String, HashMap<Integer, int[]>> SavedTimesHMPerSim;
    private HashMap<String, LinkedList<BitSet>> KeysWithBitmaps; // for estimate
    private NormalDistribution nd;
    private double[][] generator;
    private int W;
    private int d;

    private String lastkey = "";

    public WLSHsynopsis(int W, int d) {
        this.W = W;
        this.d = d;
        externalHashMap = new HashMap<>();
        Buckets = new HashMap<>();
        SavedTimesHMPerSim = new HashMap<>();
        KeysWithBitmaps = new HashMap<>();
        // Produce generator W x d.
        Random rand = new Random();
        rand.setSeed(5);
        int finalW = 3*W;
        generator = new double[finalW][d];
        for (int i = 0; i < finalW; i++) {
            for (int j = 0; j < d; j++) {
                generator[i][j] = rand.nextGaussian();
                //System.out.print( generator[i][j]+" ");
            }
        }
    }

    public void add(String key1, int time1, int[] values1) {

        //JsonNode node = (JsonNode)k;
        int[] values = new int[3];
        // ***** Take from Json Node ******
        int time = time1;
        //int alive = 0, apoptotic = 0 ,necrotic = 0;
        values[0] = values1[0];
        values[1] = values1[1];
        values[2] = values1[2];
        String key = key1;
        // *****
        HashMap<Integer, int[]> SavedTimesHM = SavedTimesHMPerSim.get(key);

        if (lastkey != key && SavedTimesHMPerSim.get(key) == null) { // erxetai neo kleidi pou den exei ksana emfanistei
            SavedTimesHM = new HashMap<Integer, int[]>(); // synepos tou dhmiourgw ena neo hashmap gia na apothikeutoun oi nees times
        }
        SavedTimesHM.put(time, values);

        TreeMap<Integer, WLSH> internalHashMap;
        internalHashMap = externalHashMap.get(key);

        if (internalHashMap == null) { // empty = true
            internalHashMap = new TreeMap<Integer, WLSH>();
            WLSH lsh = new WLSH(key, W);
            lsh.add(values);
            internalHashMap.put(time, lsh);
        } else {
            int x1 = time - W;
            int[] y1 = SavedTimesHM.get(x1);// there is a map of LSHs for this key
            for (int i = x1 + 1; i < time; i++) {

                if(y1 != null) {
                    int y3[] = LinearRegression(i, x1, y1, time, values);

                    SavedTimesHM.put(i, y3);
                }
            }
            for (Map.Entry<Integer, int[]> set : SavedTimesHM.entrySet()) {
                int savedTime = set.getKey();
                if (internalHashMap.get(savedTime) == null) {
                    //Create its lsh
                    WLSH lsh1 = new WLSH(key, W);
                    //////////fill lsh
                    WLSH lsh2 = FillLSH(savedTime, lsh1, SavedTimesHM);
                    internalHashMap.put(savedTime, lsh2);
                } else {
                    WLSH lsh1 = internalHashMap.get(savedTime);
                    if (lsh1.getCurNumData() < W) {
                        //////////fill lsh
                        WLSH lsh2 = FillLSH(savedTime, lsh1, SavedTimesHM);
                        internalHashMap.put(savedTime, lsh2);
                    }

                }
            }
        }

        externalHashMap.put(key, internalHashMap);
        SavedTimesHMPerSim.put(key, SavedTimesHM);
        //printExternal(externalHashMap);
        lastkey = key;
    }

    private int[] LinearRegression(int x, int x1, int y1[], int x2, int y2[]) {


        int[] y3 = new int[3];
        for (int i = 0; i < 3; i++) {
            double b = ((double) (y2[i] - y1[i]) / (double) (x2 - x1));
            double a = y1[i] - (double) (b * x1);
            y3[i] = (int) (a + (b * x));
        }
        return y3;
    }

    private WLSH FillLSH(int time, WLSH lsh, HashMap<Integer, int[]> hashMap) {
        int diff = W - lsh.getCurNumData();
        for (int i = 1; i <= diff; i++) {
            int pointer = lsh.getCurNumData();
            int[] values = hashMap.get(time + pointer);// mallon einai time + pointer + 1
            if (values != null) lsh.add(values);
        }
        return lsh;
    }

    public void printExternal() {

        for (Map.Entry mapElement : externalHashMap.entrySet()) {
            String key = (String) mapElement.getKey();
            System.out.println("Simulation is :" + mapElement.getKey());
            TreeMap<Integer, WLSH> value = (TreeMap<Integer, WLSH>) mapElement.getValue();
            for (Map.Entry<Integer, WLSH> set : value.entrySet()) {
                System.out.println("Time start :" + set.getKey());
                WLSH lsh = set.getValue();
                List<int[]> window_data = lsh.getWindow_data();
                System.out.println("Window Data size is: " + window_data.size());
                for (int i = 0; i < window_data.size(); i++) {
                    int[] values = window_data.get(i);
                    System.out.print("|||" + values[0] + " " + values[1] + " " + values[2]);
                }
                System.out.println("");
            }
            System.out.println("");
        }
    }

    public void printBuckets(){
        for (Map.Entry<Integer,ArrayList<String>> entry : Buckets.entrySet()) {
            System.out.print(+entry.getKey()+"th Bucket contains the simulations: ");
            for (String str : entry.getValue())
            {
                System.out.print(str+" ");
            }
            System.out.println("");
        }
    }

    private int HammingWeight(BitSet b1) {
        int weight = 0;
        for (int i = 0; i < b1.length(); i++) {
            int val = (b1.get(i)) ? 1 : 0;
            weight = weight + val;
        }
        return weight;
    }


    public HashMap<Integer, ArrayList<String>> estimate(double T) {

        for (Map.Entry<String, TreeMap<Integer, WLSH>> entry1 : externalHashMap.entrySet()) {
            LinkedList<BitSet> AllBitmaps = new LinkedList<BitSet>();

            String key = entry1.getKey(); // key = runX
            TreeMap<Integer, WLSH> AllWindows = entry1.getValue(); // oles oi xronikes stigmes tou runX

            int HW = 0;
            double arccos = Math.acos(T);
            double Th = (arccos/Math.PI)*d;
            //System.out.println("Th is "+ Th);
            int B = (int) (d / Th);
            //System.out.println("Buckets are "+ B);

            for (Map.Entry<Integer, WLSH> entry : AllWindows.entrySet()) { // Kano Hash thn kathe xroniki stigmi
               // System.out.println("For time: " + entry.getKey());
                WLSH lsh = entry.getValue();
                if (lsh.getWindow_data().size() == W) {
                    //lsh.printLSH();
                    BitSet Bitmap = lsh.estimate(generator);
                    HW = HammingWeight(Bitmap);
                    //System.out.println("Hamm Weight is:" +HW);
                    // 1st Hashing
                    int index = (int) Math.floor((double) (HW / (double) (d / B)));
                    ArrayList<String> storedKeys = Buckets.get(index);
                    if (storedKeys == null) {
                        storedKeys = new ArrayList<String>();
                    }
                    String time_to_string = entry.getKey().toString();
                    String concat_string = key.concat("_"+time_to_string);
                    storedKeys.add(concat_string);
                    Buckets.put(index, storedKeys);

                    //2nd Hashing
                    int index1 = (int) Math.floor((double) (Math.max(HW-Th,0) / (double) (d / B)));
                    if(index == index1){
                        //do nothing
                        //System.out.println("Same bucket");
                    }
                    else{
                        int diff = index -index1;
                        for(int k = 0; k<= diff-1; k++){
                            int inbetween_index = index1 + k;
                            ArrayList<String> storedKeys1 = Buckets.get(inbetween_index);
                            if (storedKeys1 == null) {
                                storedKeys1 = new ArrayList<String>();
                            }
                            storedKeys1.add(concat_string);
                            Buckets.put(inbetween_index, storedKeys1);
                        }
                    }
                    AllBitmaps.add(Bitmap);
                }
            }

            KeysWithBitmaps.put(key, AllBitmaps);
        }
        return Buckets;
    }


}