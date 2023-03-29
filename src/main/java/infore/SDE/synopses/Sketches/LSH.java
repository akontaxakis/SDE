package infore.SDE.synopses.Sketches;

import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.BitSet;
import java.util.LinkedList;

import static org.apache.commons.lang3.ArrayUtils.toArray;

public class LSH {
	//lsh sliding window
	private int W;
	//compression
	private int D;
	//workers
	private int B;
	//streamId
	private String curid;

	//current number of data
	private int nD;

	private LinkedList<Double> data;

    int lasttime;
	int interval;
	int lastvalue;


	public LSH(String id, String[] parm) {

		data = new LinkedList<>();
		curid = id;
		W = Integer.parseInt(parm[2]);
		D = Integer.parseInt(parm[3]);
		B = Integer.parseInt(parm[4]);
		nD =0;

	}

	public void add(double k){

	if(nD < W){
		data.add(k);
		nD++;
	}else{
		data.removeLast();
		data.add(k);
	}
	}

	public BitSet estimate(double[][] g){

		double [] d = multiplyByMatrix(data,g);
		return translateToBitMap(d);

	}

	public double[] multiplyByMatrix(LinkedList<Double> w, double[][] g) {
		int wLength = w.size(); // m1 columns length
		int gRowLength = g.length;    // m2 rows length
		if(wLength != gRowLength) return null; // matrix multiplication is not possible

		int dLength = g[0].length; // m result columns length
		double[] mResult = new double[dLength];
		for(int i = 0; i < gRowLength; i++) {         // rows from m1
			for(int j = 0; j < wLength; j++) {     // columns from m2
				 { // columns from m1
					mResult[i] += g[i][j] * w.get(i);
				}
			}
		}
		return mResult;
	}
	public BitSet translateToBitMap(double[] d){
		int sum=0;
		BitSet bts = new BitSet(d.length);
		for(int i = 0; i < d.length; i++){
			if(d[i]>0){
				bts.set(i,true);
			}else{
				bts.set(i,false);
			}
		}
		return bts;
	}


	public String getCurid() {
		return curid;
	}

	public void setCurid(String curid) {
		this.curid = curid;
	}
}
