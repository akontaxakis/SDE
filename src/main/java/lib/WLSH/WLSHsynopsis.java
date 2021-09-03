package lib.WLSH;

import lib.WLSH.Bucket;
import lib.WLSH.HashedWindow;
import lib.WLSH.WLSH;

import java.util.*;

public class WLSHsynopsis {
    private final int W;
    private final int d;
    private final int slide;
    double[][] generator;
    private HashMap<String, TreeMap<Integer, WLSH>> externalHashMap;
    private HashMap<Integer, Bucket> BucketsMap;
    private HashMap<String, NavigableMap<Integer, int[]>> SavedTimesHMPerSim;
    private String lastkey = "";

    public WLSHsynopsis(int W, int d) {
        this.W = W;
        this.d = d;
        slide = 10;
        externalHashMap = new HashMap<>();
        BucketsMap = new HashMap<>();
        SavedTimesHMPerSim = new HashMap<>();
        //KeysWithBitmaps = new HashMap<String, LinkedList<BitSet>>();
        // Produce generator W x d.
        Random rand = new Random();
        rand.setSeed(5);
        int finalW = 3 * W;
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
        //int alive = 0, apoptotic = 0 ,necrotic = 0;
        values[0] = values1[0];
        values[1] = values1[1];
        values[2] = values1[2];
        // *****
        NavigableMap<Integer, int[]> SavedTimesHM = SavedTimesHMPerSim.get(key1);

        if (!lastkey.equals(key1) && SavedTimesHMPerSim.get(key1) == null) { // erxetai neo kleidi pou den exei ksana emfanistei
            SavedTimesHM = new TreeMap<>(); // synepos tou dhmiourgw ena neo hashmap gia na apothikeutoun oi nees times
        }

        TreeMap<Integer, WLSH> internalHashMap = externalHashMap.get(key1);
        // STORE AND CREATE NEW VALUES

        if (internalHashMap == null) { // empty = true
            SavedTimesHM.put(time1, values);
            internalHashMap = new TreeMap<>();
            WLSH lsh = new WLSH(key1, W);
            lsh.add(values);
            internalHashMap.put(time1, lsh);
        } else {
            int x1 = SavedTimesHM.lastEntry().getKey();
            SavedTimesHM.put(time1, values);
            int[] y1 = SavedTimesHM.get(x1);// there is a map of LSHs for this key

            for (int i = x1 + 1; i < time1; i++) {
                if (y1 != null) {
                    int[] y3 = LinearRegression(i, x1, y1, time1, values);
                    SavedTimesHM.put(i, y3);
                }
            }
            // CREATE LSH FOR THE STORED VALUES
            for (Map.Entry<Integer, int[]> set : SavedTimesHM.entrySet()) {
                int savedTime = set.getKey();
                if(savedTime%slide==0) {
                    WLSH lsh1 = internalHashMap.get(savedTime);
                    if (lsh1 == null) {
                        lsh1 = new WLSH(key1, W);
                        WLSH lsh2 = FillLSH(savedTime, lsh1, SavedTimesHM);
                        internalHashMap.put(savedTime, lsh2);
                    } else {
                        if (lsh1.getCurNumData() < W) {
                            WLSH lsh2 = FillLSH(savedTime, lsh1, SavedTimesHM);
                            internalHashMap.put(savedTime, lsh2);
                        }
                    }
                }
            }
        }

        externalHashMap.put(key1, internalHashMap);
        SavedTimesHMPerSim.put(key1, SavedTimesHM);
        //printExternal(externalHashMap);
        lastkey = key1;
    }

    public int[] LinearRegression(int x, int x1, int[] y1, int x2, int[] y2) {

        int[] y3 = new int[3];
        for (int i = 0; i < 3; i++) {
            double b = ((double) (y2[i] - y1[i]) / (double) (x2 - x1));
            double a = y1[i] - (b * x1);
            y3[i] = (int) (a + (b * x));
        }
        return y3;
    }

    public WLSH FillLSH(int time, WLSH lsh, NavigableMap<Integer, int[]> hashMap) {
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
            System.out.println("Simulation is :" + mapElement.getKey());
            TreeMap<Integer, WLSH> value = (TreeMap<Integer, WLSH>) mapElement.getValue();
            for (Map.Entry<Integer, WLSH> set : value.entrySet()) {
                System.out.println("Time start :" + set.getKey());
                WLSH lsh = set.getValue();
                List<int[]> window_data = lsh.getWindow_data();
                System.out.println("Window Data size is: " + window_data.size());
                for (int[] values : window_data) {
                    System.out.print("|||" + values[0] + " " + values[1] + " " + values[2]);
                }
                System.out.println("");
            }
            System.out.println("");
        }
    }

    public int HammingWeight(BitSet b1) {
        int weight = 0;
        for (int i = 0; i < b1.length(); i++) {
            int val = b1.get(i) ? 1 : 0;
            weight = weight + val;
        }
        return weight;
    }

    private HashMap<Integer, Bucket> initializeBuckets(double Th, int numofBuckets) {
        for (int g = 0; g < numofBuckets; g++) {
            // Initialize all indexes
            Bucket storedKeys = new Bucket(Th, 10);
            BucketsMap.put(g, storedKeys);
        }
        Bucket storedKeys = new Bucket(Th, 10);
        BucketsMap.put(numofBuckets, storedKeys);
        return BucketsMap;
    }


    public HashMap<Integer, Bucket> estimate(double T) {

        // T is Threshold
        double Th = (Math.acos(T) / Math.PI) * d;

        int B = (int) (d / Th);
        System.out.println("NUM OF BUCKETS IS " + B);
        BucketsMap = initializeBuckets(Th, B);

        for (Map.Entry<String, TreeMap<Integer, WLSH>> entry1 : externalHashMap.entrySet()) {

            String key = entry1.getKey(); // key = runX
            NavigableMap<Integer, int[]> integerNavigableMap = SavedTimesHMPerSim.get(key);
            int[] maxes = findMaxPerColumn(integerNavigableMap);

            TreeMap<Integer, WLSH> AllWindows = entry1.getValue(); //  all windows of size W in this simulation

            for (Map.Entry<Integer, WLSH> entry : AllWindows.entrySet()) { // Kano Hash thn kathe xroniki stigmi

                WLSH lsh = entry.getValue();

                if (lsh.getWindow_data().size() == W) {
                    int HW;
                    BitSet Bitmap = lsh.estimate(generator,maxes);
                    HW = HammingWeight(Bitmap);
                    // 1st Hashing
                    int index = (int) Math.floor(HW / Th);
                    HashedWindow h_window = new HashedWindow(key, entry.getKey(), HW, lsh);
                    BucketsMap = add_in_primary(h_window, index);

                    //2nd Hashing
                    int index1 = (int) Math.floor(Math.max(HW - Th, 0) / Th);
                    if (index != index1) {
                        int diff = index - index1;
                        for (int k = 0; k <= diff - 1; k++) {
                            int inbetween_index = index1 + k;
                            BucketsMap = add_in_foreign(h_window, inbetween_index);
                        }
                    }
                }
            }
        }

        return BucketsMap;
    }

    private int[] findMaxPerColumn(NavigableMap<Integer,int[]> integerNavigableMap) {
        int[] maxes = new int[3];
        for (Map.Entry<Integer,int[]> set :integerNavigableMap.entrySet()){
            int[] array = set.getValue();
            for(int i = 0; i<array.length; i++){
                if(array[i]>maxes[i]){
                    maxes[i] = array[i];
                }
            }
        }
        return maxes;
    }

    private HashMap<Integer, Bucket> add_in_primary(HashedWindow hw, int index) {

        Bucket hashedWindows = BucketsMap.get(index);
        hashedWindows.getPrimaryList().add(hw);
        BucketsMap.put(index, hashedWindows);
        return BucketsMap;
    }

    private HashMap<Integer, Bucket> add_in_foreign(HashedWindow hw, int index) {

        Bucket hashedWindows = BucketsMap.get(index);
        hashedWindows.getForeignList().add(hw);
        BucketsMap.put(index, hashedWindows);
        return BucketsMap;
    }

    public void printBuckets() {
        System.out.println("Let's get into the buckets");
        for (Map.Entry<Integer, Bucket> set : BucketsMap.entrySet()) {
            System.out.println();
            System.out.println("BUCKET INDEX: " + set.getKey());
            set.getValue().printPrimaryList();
            set.getValue().printForeignList();
            System.out.println();
        }

    }
}