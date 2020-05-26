package infore.SDE.synopses;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import Coresets.BucketManager;
import Coresets.Point;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

public class FinJoinCoresets extends Synopsis{

	private HashMap<String, ArrayList<Double>> Counters;
	private int d;
	private BucketManager bucketManager;
	private String [] par;

	public FinJoinCoresets(int uid, String[] parameters) {
		super(uid, parameters[0], parameters[1]);
		Random random = new Random(1);
		d = Integer.parseInt(parameters[2]);
		par = parameters;
	    bucketManager = new BucketManager(Integer.parseInt(parameters[3]),Integer.parseInt(parameters[2]),random );
	    Counters = new HashMap<String, ArrayList<Double>>();
	}

	@Override
	public void add(Object k) {
		// TODO Auto-generated method stub
		String j = (String)k;
		String[] tokens = j.split(",");



			ArrayList<Double> c = Counters.get(tokens[this.keyIndex]);
			
			if (c == null)
				c = new ArrayList<Double>();
			

				c.add(Double.parseDouble(tokens[this.valueIndex]));

				if(c.size() == d){
				double [] coordinates = new double[d];	
				//if(c.size()<d) {
					for(int i = 0;i<d;i++) {
						if(i<c.size()) {
							coordinates[i]=c.get(i);
						}else {
							coordinates[i]=0.0;
						}

					}

				Point value = new Point(coordinates);
				bucketManager.insertPoint(value);

				Counters.put(tokens[this.keyIndex], new ArrayList<>() );

			}else{
					Counters.put(tokens[this.keyIndex], c );
				}
				
		}
	

	@Override
	public Object estimate(Object k) {
		// TODO Auto-generated method stub
		Point [] partialCoreset = bucketManager.getCoresetFromManager();
		return (Object)partialCoreset;
	}
	@Override
	public Estimation estimate(Request rq) {

		Point [] partialCoreset = bucketManager.getCoresetFromManager();
		return new Estimation(rq, (Object)partialCoreset, Integer.toString(rq.getUID()));
	}
	
	
	@Override
	public Synopsis merge(Synopsis sk) {
		// TODO Auto-generated method stub
		return null;
	}

}
