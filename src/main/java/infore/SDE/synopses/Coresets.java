package infore.SDE.synopses;

import java.util.Random;

import lib.Coresets.BucketManager;
import lib.Coresets.Point;
import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

public class Coresets extends Synopsis{

	private int d;
	private BucketManager bucketManager;
	
	public Coresets(int uid, String[] parameters) {
		super(uid, parameters[0], parameters[1], parameters[2]);

		Random random = new Random();
		d = Integer.parseInt(parameters[3]);
	    bucketManager = new BucketManager(Integer.parseInt(parameters[4]),Integer.parseInt(parameters[5]),random);

	}

	@Override
	public void add(Object k) {
		// TODO Auto-generated method stub
		JsonNode node = (JsonNode)k;
		String value = node.get(this.valueIndex).asText();
		double [] coordinates = new double[d];
		
		for(int i=0; i < d; i++) {
			coordinates[i] = Double.parseDouble(value);
		}
				
		Point pvalue = new Point(coordinates);
		bucketManager.insertPoint(pvalue);
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
