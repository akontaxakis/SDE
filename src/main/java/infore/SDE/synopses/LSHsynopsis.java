package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.RandomGenerator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Random;


public class LSHsynopsis extends Synopsis{

	private HashMap<String, LSH> Synopses;
	private NormalDistribution nd;
	private int W;
	private int D;
	private int B;
	String[] parameters;


	public LSHsynopsis(int uid, String[] param) {
		super(uid, param[0], param[1]);

		W = Integer.parseInt(param[2]);
		D = Integer.parseInt(param[3]);
		B = Integer.parseInt(param[4]);

		nd =  new NormalDistribution(0, 0,1 );

		Synopses = new HashMap<>();
		parameters = param;
		// TODO Auto-generated constructor stub
	}

	@Override
	public void add(Object k) {

		JsonNode node = (JsonNode)k;

		String key = node.get(this.keyIndex).asText();
		LSH lsh = Synopses.get(key);
		if(lsh == null)
			lsh = new LSH(key,parameters);

		lsh.add(node.get(this.valueIndex).asDouble());
		Synopses.put(key, lsh);

	}

	@Override
	public Object estimate(Object k) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Synopsis merge(Synopsis sk) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Estimation estimate(Request rq) {

		double[][] g = new double[W][D];

		for (int r=0;r<W;r++){
			for (int c=0;c<D;c++) {
				g[r][c] = nd.sample();
			}
		}

		double th =  Double.parseDouble(rq.getParam()[0]);
		double arTh = (Math.acos(th)*(double)D)/Math.PI;

		ArrayList<Bitmap> Output = new ArrayList<>();


		for( LSH lsh:  Synopses.values()) {
			BitSet bits = lsh.estimate(g);
			Output.add(new Bitmap(lsh.getCurid(),bits,calHamWeight(bits)));
		}

		return new Estimation(rq, Output, Integer.toString(rq.getUID()));

		/*
		double workid =Math.floor(weight/(D/B));

		double workid2;
		if(weight - arTh> 0){
			workid2 = Math.floor((weight - arTh)/(D/B));
		}else{
			workid2 =0;
		}


		 */
	}

	public double calHamWeight(BitSet bts){
		double sum=0;

		for(int i = 0; i < bts.size(); i++){
			if(bts.get(i)){
				sum++;
			}
		}
		return sum;
	}



}
