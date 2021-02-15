package infore.SDE.synopses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import Coresets.BucketManager;
import Coresets.Point;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

public class FinJoinCoresets extends Synopsis{

	private HashMap<String, ArrayList<Double>> Counters;
	private int d;
	private BucketManager bucketManager;
	private String [] par;

	public FinJoinCoresets(int uid, String[] parameters) {
		super(uid, parameters[0], parameters[1], parameters[2]);
		Random random = new Random(1);
		d = Integer.parseInt(parameters[3]);
		par = parameters;
	    bucketManager = new BucketManager(Integer.parseInt(parameters[4]),Integer.parseInt(parameters[5]),random );
	    Counters = new HashMap<String, ArrayList<Double>>();
	}

	@Override
	public void add(Object k) {
		// TODO Auto-generated method stub
		//ObjectMapper mapper = new ObjectMapper();
		JsonNode node = (JsonNode)k;
        /*try {
            node = mapper.readTree(j);
        } catch (IOException e) {
            e.printStackTrace();
        } */
		String key = node.get(this.keyIndex).asText();
		String value = node.get(this.valueIndex).asText();



			ArrayList<Double> c = Counters.get(key);
			
			if (c == null)
				c = new ArrayList<Double>();
			

				c.add(Double.parseDouble(value));

				if(c.size() == d){
				double [] coordinates = new double[d];	
				//if(c.size()<d) {
					for(int i = 0;i<d;i++) {
							coordinates[i]=c.get(i);
					}

				Point pvalue = new Point(coordinates);
				bucketManager.insertPoint(pvalue );

				Counters.put(key, new ArrayList<>() );

			}else{
					Counters.put(key, c );
				}
				
		}
	

	@Override
	public Object estimate(Object k) {
		// TODO Auto-generated method stub
		Point [] partialCoreset = bucketManager.getCoresetFromManager();
		return partialCoreset;
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
