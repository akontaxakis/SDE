package lib.WLSH;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Bucket {

    private final double Th;
    private final int K;
    private String BucketID;
    private ArrayList<HashedWindow> PrimaryList;
    private ArrayList<HashedWindow> ForeignList;
    private ArrayList<SimilarSims> CorrelatedSims;


    public Bucket(double th, int k) {
        K = k;
        Th = th;
        PrimaryList = new ArrayList<>();
        ForeignList = new ArrayList<>();
        CorrelatedSims = new ArrayList<>();
    }

    public Bucket(double th, String y, int k) {
        K = k;
        BucketID = y;
        Th = th;
        PrimaryList = new ArrayList<>();
        ForeignList = new ArrayList<>();
        CorrelatedSims = new ArrayList<>();
    }

    public ArrayList<HashedWindow> getPrimaryList() {
        return PrimaryList;
    }

    public void setPrimaryList(ArrayList<HashedWindow> primaryList) {
        PrimaryList = primaryList;
    }

    public ArrayList<HashedWindow> getForeignList() {
        return ForeignList;
    }

    public void setForeignList(ArrayList<HashedWindow> foreignList) {
        ForeignList = foreignList;
    }

    public void printPrimaryList() {
        System.out.println("The PrimaryList of the Bucket");
        for (HashedWindow hashedWindow : PrimaryList) {
            System.out.print(hashedWindow.getSimulation() + "-" + hashedWindow.getStart_time() + "|||");
        }
        System.out.println();
    }

    public void printForeignList() {
        System.out.println("The ForeignList of the Bucket");
        for (HashedWindow hashedWindow : ForeignList) {
            System.out.print(hashedWindow.getSimulation() + "-" + hashedWindow.getStart_time() + "|||");
        }
        System.out.println();
    }

    public HashMap<String,ArrayList<SimilarSims>> produceTopKPerSim(int kap, double Threshold,ArrayList<String> list,int T){
        HashMap<String,ArrayList<SimilarSims>> map = new HashMap<>();
        for(String s : list){
            String[] s1 = s.split("\\_");
            String simulation = s1[0];
            String start_time = s1[1];
            int time = Integer.parseInt(start_time);
            HashMap<String,ArrayList<SimilarSims>> kOneMap = produceTopKForOneSim(kap,Threshold,simulation,time,T);
            if(kOneMap!=null) {
                ArrayList<SimilarSims> kOnelist = kOneMap.get("1");
                if (kOnelist != null) {
                    map.put(s, kOnelist);
                }
            }
        }
        return map;
    }


    public HashMap<String,ArrayList<SimilarSims>> produceTopKForOneSim(int kap, double Threshold, String simulation, int start_time, int T){
        HashMap<String,ArrayList<SimilarSims>> topMap = new HashMap<>();
        System.out.println("START Query2 FOR BUCKET " + toString() + " in time " + java.time.LocalTime.now());
        ArrayList<SimilarSims> SimilarList = new ArrayList<>();
        ArrayList<SimilarSims> TopKList = new ArrayList<>();
        HashedWindow sim_hw;
        boolean exist = false;
        int x = 0;
        for (HashedWindow hw : PrimaryList) {
            x++;
            if (hw.getSimulation().equals(simulation) && hw.getStart_time() == (start_time)) { // the simulation exists in primary list
                exist = true;
                for (HashedWindow hw1 : PrimaryList) {
                    if (!(hw.getSimulation().equals(hw1.getSimulation()))){
                        double HWdiff = Math.abs(hw.getHammingWeight() - hw1.getHammingWeight());
                        SimilarSims ss1 = new SimilarSims(hw, hw1, HWdiff);
                        double corr = ss1.computeCorrelation();
                        if (hw1.getStart_time() < (1440 - T)) {
                            if (corr >= Threshold) {
                                //System.out.println("PL"+ss1.toString());
                                SimilarList.add(ss1);
                            }
                        }
                    }
                }
                for (HashedWindow hw2 : ForeignList){
                    double HWdiff = Math.abs(hw.getHammingWeight() - hw2.getHammingWeight());
                    SimilarSims ss2 = new SimilarSims(hw,hw2,HWdiff);
                    double corr = ss2.computeCorrelation();
                    if(hw2.getStart_time() < (1440 - T)){
                        if(corr>=Threshold){
                            //System.out.println("FL"+ss2.toString());
                            SimilarList.add(ss2);
                        }
                    }
                }
                SimilarList.sort(Collections.reverseOrder());
                /*List<SimilarSims> personListFiltered = SimilarList.stream()
                        .filter(distinctByKey(p -> p.getSim2()))
                        .collect(Collectors.toList());
                */
                if(SimilarList.size()>kap){
                    for(int i = 0; i<kap; i++){
                        TopKList.add(SimilarList.get(i));
                    }
                }
                else{
                    System.out.println("FINAL Bucket INFO -> " + toString() + "in time " + java.time.LocalTime.now());
                    topMap.put("1",SimilarList);
                    return topMap;
                }
                System.out.println("FINAL Bucket INFO -> " + toString() + "in time " + java.time.LocalTime.now());
                topMap.put("1",TopKList);
                return topMap;
            }
        }


        System.out.println("FINAL Bucket INFO -> " + toString() + "in time " + java.time.LocalTime.now());
        return null;

    }

    public static  ArrayList<SimilarSims> removeDuplicates(ArrayList<SimilarSims> list) {

        // Create a new ArrayList
        ArrayList<SimilarSims> newList = new ArrayList<SimilarSims>();

        // Traverse through the first list
        for (SimilarSims element : list) {

            // If this element is not present in newList
            // then add it
            if (!newList.contains(element)) {

                newList.add(element);
            }
        }

        // return the new list
        return newList;
    }

    public static <T> Predicate<T> distinctByKey(
            Function<? super T, ?> keyExtractor) {

        Map<Object, Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }


    // Function that do the comparisons between two lists
    public HashMap<String,ArrayList<SimilarSims>> compareForSimilarity(int kap, double Threshold) {

        HashMap<String,ArrayList<SimilarSims>> topmap = new HashMap<>();
        ArrayList<SimilarSims> topList = new ArrayList<>();
        HashMap<Integer,Integer> map = new HashMap<>();
        System.out.println("START COMPARISONS FOR BUCKET " + toString() + " in time " + java.time.LocalTime.now());

        SimilarSims[] mostSimilar = new SimilarSims[kap];
        double correlation = 0.0;
        for (int i = 0; i < mostSimilar.length; i++) {
            mostSimilar[i] = new SimilarSims(null, null, Th, correlation);
        }
        boolean f1 = false, f2 = false, f3 = false;
        double maxdiff = largest(mostSimilar);
        // Compare PrimaryList with itself
        System.out.println("PrimaryList.size():" + PrimaryList.size() + " |" + "ForeignList.size():" + ForeignList.size());
        int y = 0, x = 0;
        for (HashedWindow hwi : PrimaryList) {

            if ((!f1) && y >= PrimaryList.size() * 0.1) {
                System.out.println("above 10% in time " + java.time.LocalTime.now());
                f1 = true;
            }
            if ((!f2) && y >= PrimaryList.size() * 0.5) {
                f2 = true;
                System.out.println("above 50% in time " + java.time.LocalTime.now());
            }
            if ((!f3) && y >= PrimaryList.size() * 0.9) {
                f3 = true;
                System.out.println("above 90% in time " + java.time.LocalTime.now());
            }

            x = 0;
            for (HashedWindow hwj : PrimaryList) {
                if (y != x) {
                    if (map.containsKey(x) && map.get(x) == y) {
                    }
                    else {
                        double mindiff = Math.abs(hwi.getHammingWeight() - hwj.getHammingWeight());
                        if (mindiff <= maxdiff) {
                            int index = findIndex(mostSimilar, maxdiff);
                            mostSimilar[index].setInit(true);
                            mostSimilar[index].setHWdiff(mindiff);
                            mostSimilar[index].setSim1(hwi);
                            mostSimilar[index].setSim2(hwj);
                            maxdiff = largest(mostSimilar);
                        }
                    }
                }
                x++;
            }
            y++;
        }

        // Compare PrimaryList with ForeignList
        for (HashedWindow hashedWindow : PrimaryList) {
            for (HashedWindow window : ForeignList) {
                double mindiff = Math.abs(hashedWindow.getHammingWeight() - window.getHammingWeight());
                maxdiff = largest(mostSimilar);
                if (mindiff <= maxdiff) {
                    System.out.println();
                    int index = findIndex(mostSimilar, maxdiff);
                    mostSimilar[index].setInit(true);
                    mostSimilar[index].setHWdiff(mindiff);
                    mostSimilar[index].setSim1(hashedWindow);
                    mostSimilar[index].setSim2(window);
                }
            }
        }

        SimilarSims[] array = removeUnCorrelated(Threshold, mostSimilar);
        for (SimilarSims similarSims : array) {
            topList.add(similarSims);
            System.out.println(similarSims.toString());
        }
        topmap.put("1",topList);
        System.out.println("FINAL Bucket INFO -> " + toString() + "in time " + java.time.LocalTime.now());

        return topmap;
    }


    private SimilarSims[] removeUnCorrelated(double Threshold, SimilarSims[] mostSimilar) {

        List<SimilarSims> list = new ArrayList<>();

        for (SimilarSims similarSims : mostSimilar) {
            if (similarSims.getSim1() != null) {
                if (similarSims.computeCorrelation() > Threshold) {
                    list.add(similarSims);
                }
            }
        }

        mostSimilar = list.toArray(new SimilarSims[list.size()]);
        return mostSimilar;
    }

    private int findIndex(SimilarSims[] mostSimilar, double max) {

        int index = 0; //index of max value

        for (int i = 0; i < mostSimilar.length; i++) {
            if (mostSimilar[i].getHWdiff() == max) {
                index = i;
                break;
            }
        }
        return index;
    }

    private double largest(SimilarSims[] arr) {
        int i;
        // Initialize maximum element
        double max = arr[0].getHWdiff();

        // Traverse array elements from second and
        // compare every element with current max
        for (i = 1; i < arr.length; i++)
            if (arr[i].getHWdiff() > max)
                max = arr[i].getHWdiff();

        return max;
    }


    public void add(Bucket bt) {
        // Concatinate the same lists
        PrimaryList.addAll(bt.getPrimaryList());
        ForeignList.addAll(bt.getForeignList());
    }


    @Override
    public String toString() {
        return "Bucket{" +
                "BucketID='" + BucketID + '\'' +
                ", PrimaryList=" + PrimaryList.size() +
                ", ForeignList=" + ForeignList.size() +
                ", Th=" + Th +
                ", K=" + K +
                '}';
    }

}
