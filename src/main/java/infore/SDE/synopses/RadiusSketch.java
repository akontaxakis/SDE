package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Datapoint;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;

public class RadiusSketch extends ContinuousSynopsis {

    private int number_of_groups;
    private int group_size;
    private int sketch_size;
    private String Datasetid;
    private Random rd;
    private double[][] r;
    private int window_size;

    public RadiusSketch(int uid, String parameter, String parameter1, String parameter2) {
        super(uid, parameter, parameter1, parameter2);
    }

    @Override
    public Estimation addEstimate(Object k) {
        return null;
    }

    public RadiusSketch(int uid, String parameter, String parameter1, String parameter2, String id, String[] param) {
        super(uid, parameter, parameter1, parameter2);
        Datasetid = id;
        number_of_groups = Integer.parseInt(param[2]);
        window_size = Integer.parseInt(param[5]);
        group_size = Integer.parseInt(param[3]);
        sketch_size = Integer.parseInt(param[4]);
        r = new double[window_size][sketch_size];

        rd =  new Random(100);
        for(int i=0;i<window_size;i++){
            for(int j=0;j<sketch_size;j++){
                if(rd.nextBoolean()==true){
                    r[i][j] = 1;
                }else{
                    r[i][j] = -1;
                }
            }
        }
    }


    @Override
    public void add(Object k) {

    }

    @Override
    public Object estimate(Object k) {
        JsonNode node = (JsonNode) k;
        ArrayList<Datapoint> dps = new ArrayList<>();

        String key = node.get(this.keyIndex).asText();
        String values = node.get(this.valueIndex).asText();
        String[] prices = values.split(";");

        ArrayList<Double> ps = new ArrayList<>();
        for(int p =0;p< prices.length;p++){
            ps.add(Double.parseDouble(prices[p]));
        }
        double[] sketch = multiplyByMatrix(ps,r);
        for (int j = 0; j < number_of_groups; j++) {
            Datapoint dp;
            if(j*2+1<sketch_size) {
                dp = new Datapoint(Datasetid + "_" + j, key, (int)sketch[j*2] + ";" + (int)sketch[j*2+1]);
            }else{
                dp = new Datapoint(Datasetid + "_" + j, key, (int)sketch[j] + ";" + (int)sketch[j - group_size]);
            }

            dps.add(dp);
        }
        return dps;
    }

    public double[] multiplyByMatrix(ArrayList<Double> w, double[][] g) {
        int wLength = w.size(); // m1 columns length
        int gRowLength = g.length;    // m2 rows length
        if(wLength != gRowLength) return null; // matrix multiplication is not possible

        int dLength = g[0].length; // m result columns length

        double[] mResult = new double[dLength];
        for(int i = 0; i < dLength; i++) {         // rows from m1
            for(int j = 0; j < wLength; j++) {     // columns from m2
                { // columns from m1
                    mResult[i] += g[j][i] * w.get(j);
                }
            }
        }
        return mResult;
    }


    @Override
    public Estimation estimate(Request rq) {
        return null;
    }

    @Override
    public Synopsis merge(Synopsis sk) {
        return null;
    }
}
