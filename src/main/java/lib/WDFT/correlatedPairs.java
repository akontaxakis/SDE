package lib.WDFT;

import java.util.*;

public class correlatedPairs {

    private TreeMap<Integer, PAIR> mapAll;//<correlation,[StreamID1,StreamID2]>
    private TreeMap<Integer, PAIR> mapOnSameWin;//<correlation,[StreamID1,StreamID2]>
    private TreeMap<String, LinkedList<PAIR>> mapWithID;
    private int keyToAll;
    private int keyToWin;

    public correlatedPairs() {
        mapAll = new TreeMap<>();
        mapOnSameWin = new TreeMap<>();
        mapWithID = new TreeMap<>();
        keyToAll = 0;
        keyToWin = 0;
    }

    public void addToMap(PAIR pr) {
        mapAll.put(keyToAll++, pr);
    }

    public void addToMapID(LinkedList<PAIR> pr, String streamId) {
        mapWithID.put(streamId, pr);
    }

    public void addOnSameWin(PAIR pr) {
        mapOnSameWin.put(keyToWin++, pr);
    }

    public LinkedList<PAIR> topK(int k, double threshold) {
        LinkedList<PAIR> topK = new LinkedList<>();
        for (Map.Entry<Integer, PAIR> pair: mapAll.entrySet()) {
            PAIR pair1 = pair.getValue();
            double pearson = pair1.pearsonCorr();
            if (pearson >= threshold) {
                topK.add(pair1);
                if (topK.size() == k) {
                    //System.out.println("****** Similar list size: ****** " + topK.size());
                    return topK;
                }
            }
        }
        System.out.println("****** Similar list size: ****** " + topK.size());
        return topK;
    }
    ///////////////////////////////////////////////////////////////////////
    public LinkedList<PAIR> topKSameWin(int k, double threshold) {
        LinkedList<PAIR> topK = new LinkedList<>();
        for (Map.Entry<Integer, PAIR> pair: mapOnSameWin.entrySet()) {
            PAIR pair1 = pair.getValue();
            if (pair1.pearsonCorr() >= threshold) {
                topK.add(pair1);
                if (topK.size() == k) {
                    System.out.println("****** Similar list size: ****** " + topK.size());
                    return topK;
                }
            }
        }
        System.out.println("****** Similar list size: ****** " + topK.size());
        return topK;
    }
}
