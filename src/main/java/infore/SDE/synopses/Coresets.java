package infore.SDE.synopses;

import java.util.Random;

import Coresets.BucketManager;
import Coresets.Point;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

public class Coresets extends Synopsis{

	private int d;
	private BucketManager bucketManager;
	
	public Coresets(int uid, String[] parameters) {
		super(uid, parameters[0], parameters[1]);
		Random random = new Random();
		d = Integer.parseInt(parameters[2]);
	    bucketManager = new BucketManager(Integer.parseInt(parameters[2]),Integer.parseInt(parameters[3]),random );
	}

	@Override
	public void add(Object k) {
		// TODO Auto-generated method stub
		String j = (String)k;
		String[] tokens = j.split(",");
		double [] coordinates = new double[d];
		
		for(int i=0; i < d; i++) {
			coordinates[i] = Double.parseDouble(tokens[i]);
		}
				
		Point value = new Point(coordinates);
		bucketManager.insertPoint(value);
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
