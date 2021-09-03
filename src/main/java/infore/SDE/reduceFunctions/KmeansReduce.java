package infore.SDE.reduceFunctions;

import lib.Coresets.KmeansPP;
import lib.Coresets.Point;
import lib.Coresets.TreeCoreset;
import infore.SDE.messages.Estimation;

import java.util.ArrayList;
import java.util.Random;

public class KmeansReduce extends ReduceFunction{

    ArrayList<Point[]> points;

    public KmeansReduce(int nOfP, int count, String[] parameters, int syn, int rq) {
        super(nOfP, count, parameters, syn, rq);
        points = new ArrayList<>();
        // TODO Auto-generated constructor stub
    }

    @Override
    public boolean add(Estimation e) {
        ArrayList<Point[]> k = (ArrayList<Point[]>) e.getEstimation();
        count++;
        for(Point[] c: k) {
            points.add(c);
        }

        return false;
    }

    @Override
    public Object reduce() {
        Random random = new Random(1);

        int rc =0;
        Point[] finalCorest = null;
        for (Point[] value : points) {

            if(rc == 0){
                rc++;
                finalCorest = value;
            }
            else if (finalCorest.length > 0)
                finalCorest = TreeCoreset.unionTreeCoreset(finalCorest, value, Integer.parseInt(parameters[3]), Integer.parseInt(parameters[2]), random);
        }
        Point[] centroids = KmeansPP.applyKmeansPP(finalCorest, 1, -1, Integer.parseInt(parameters[4]), Integer.parseInt(parameters[2]), random);

        //Build string format "[weight x y z ...]"
        StringBuilder sb = new StringBuilder();
        for(Point p : centroids){
            sb.append(p.toString()).append("\n");
        }

        return sb.toString();
    }


}
