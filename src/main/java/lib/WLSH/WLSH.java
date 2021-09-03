package lib.WLSH;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

public class WLSH {

    //size of sliding window
    private final int W;
    //data
    private LinkedList<int[]> window_data;
    // compression rate
    //private int d;
    //id of current simulation
    private String curid;

    //number of data up to now
    private int curNumData;

    public WLSH(String key, int w) {
        window_data = new LinkedList<>();
        W = w;
        //this.d = d;
        curid = key;
        curNumData = 0;
    }

    public String getCurid() {
        return curid;
    }

    public void setCurid(String curid) {
        this.curid = curid;
    }

    public List<int[]> getWindow_data() {
        return window_data;
    }

    public void add(int[] value) {

        window_data.add(value);
        curNumData++;
    }

    public int getCurNumData() {
        return curNumData;
    }

    public void printLSH() {
        System.out.println("Size of lsh: " + window_data.size());
        for (int[] values : window_data) {
            System.out.print("|||" + values[0] + " " + values[1] + " " + values[2]);
        }
    }

    public BitSet estimate(double[][] random_gen, int[] maxes) { // returns BitMap
        double[] mul_result = calcMatrixR(window_data, random_gen,maxes);
        return convertToBitMap(mul_result);
    }

    public int[] ToOneDimensionalArray(LinkedList<int[]> data, int[] maxes) {
        int[][] TwoDimArray = new int[W][3];
        int size = data.size();
        int oneDimSize = 3 * size;

        for (int i = 0; i < size; i++) {
            int[] values = data.get(i); // Nea 3ada alive, apoptotic, necrotic
            for (int j = 0; j < 3; j++) {
                TwoDimArray[i][j] = values[j];
            }
        }
        //Normalize Data
        int[][] normArray = normalizeData(TwoDimArray,maxes);
        List<Integer> list = new ArrayList<Integer>();
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < size; i++) {
                list.add(TwoDimArray[i][j]);
            }
        }

        int[] OneDimArray = new int[list.size()];
        for (int i = 0; i < OneDimArray.length; i++) {
            OneDimArray[i] = list.get(i);
        }

        return OneDimArray;
    }

    private int[][] normalizeData(int[][] twoDimArray, int[] maxes) {
        //int[][] array = new int[W][3];

        for(int j = 0; j<3; j++){
            for(int i = 0; i<W; i++){
                twoDimArray[i][j] = twoDimArray[i][j] - maxes[j]/2;
            }
        }
        return twoDimArray;
    }

    private double[] calcMatrixR(LinkedList<int[]> data, double[][] random_gen, int[] maxes) {

        int[] values = ToOneDimensionalArray(data,maxes);
        int data_length = values.length;
        int random_gen_rows = random_gen.length;
        ///System.out.println("Rows of gen are:" +random_gen_rows);
        if (random_gen_rows != data_length) return null;
        int random_gen_col = random_gen[0].length;
        //System.out.println("Columns of gen are:" +random_gen_col);
        double[] mul = new double[random_gen_col];

        for (int i = 0; i < random_gen_col; i++) { // columns
            for (int j = 0; j < random_gen_rows; j++) { // rows
                mul[i] += (random_gen[j][i] * values[j]); // ALIVE
            }
        }
        return mul;
    }

    public BitSet convertToBitMap(double[] mul) {
        int sizeOfBitset = mul.length;
        BitSet b1 = new BitSet(sizeOfBitset);
        for (int i = 0; i < sizeOfBitset; i++) {
            if (mul[i] > 0) {
                b1.set(i, true);
            } else {
                b1.set(i, false);
            }
        }
        return b1;
    }

}
