package infore.SDE.synopses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.math3.complex.Complex;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

public class FinJoinSynopsis extends Synopsis{
	
	private  HashMap<String, Integer> Counters;
	private int dataLevelIndex;
	private  HashMap<String, Synopsis> Synopses;
	private String[] parameters;
	
	public FinJoinSynopsis(int uid, String[] param) {
		super(uid, param[0], param[1]);
		dataLevelIndex = Integer.parseInt(param[5]);
		Synopses = new HashMap<String, Synopsis>();
		Counters = new HashMap<String, Integer>();
		parameters = param;
		// TODO Auto-generated constructor stub
	}

	@Override
	public void add(Object k) {
		String j = (String)k;
		// TODO Auto-generated method stub

		ObjectMapper mapper = new ObjectMapper();
		JsonNode node = null;
		try {
			node = mapper.readTree(j);
		} catch (IOException e) {
			e.printStackTrace();
		}
		String key = node.get(this.keyIndex).asText();
		String value = node.get(this.valueIndex).asText();

		char last = j.charAt(j.length() -1);
		//System.out.println(last);
		if(last == '1') {
			
			Integer count = Counters.get(key);
			if(count == null)
				count = 1;
			else
				count++;	
			
		Counters.put(key, count);
		}
		else if(last == '2') {
		
			Synopsis DFT = Synopses.get(key);
			
			if(DFT == null)
				DFT = new DFT(this.SynopsisID,parameters,key);
			Integer countSofar = Counters.get(key);
			
			if (countSofar!=null) {
				//System.out.println(countSofar);
				DFT.add(countSofar +"");
				Synopses.put(key, DFT);
				Counters.put(key, 0);
		}else {
				Counters.put(key, 0);
		      }
		}
		
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
		/*
		HashMap<String, ArrayList<Complex[]>> Output = new HashMap<>();
		// TODO Auto-generated method stub
	    //Iterator<Entry<String, Synopsis>> it = Synopses.entrySet().iterator();
	    for(Map.Entry<String, Synopsis> pair:  Synopses.entrySet()) {
	   
	    	DFT df = (DFT) pair.getValue();
	    	Complex[] ncoef = df.getCOEF();
	    	ArrayList<String> str = df.getKeys2(Double.parseDouble(rq.getParam()[0]));
	    	for(String entry: str) {
	    		ArrayList<Complex[]> tmp =	Output.get(entry);
	    		if(tmp == null) {
	    			tmp = new ArrayList<Complex[]>();
	    		}
	    		tmp.add(ncoef);
	    		Output.put(entry, tmp);
	    	}
	    }
		return new Estimation(rq, Output, Integer.toString(rq.getUID())); */
		return null;
	}

public HashMap<String, ArrayList<Complex[]>> estimate2(Request rq) {
		/*
		HashMap<String, ArrayList<Complex[]>> Output = new HashMap<>();
		// TODO Auto-generated method stub
	    //Iterator<Entry<String, Synopsis>> it = Synopses.entrySet().iterator();
	    for(Map.Entry<String, Synopsis> pair:  Synopses.entrySet()) {
	   
	    	DFT df = (DFT) pair.getValue();
	    	Complex[] ncoef = df.getCOEF();
	    	ArrayList<String> str = df.getKeys2(Double.parseDouble(rq.getParam()[0]));
	    	for(String entry: str) {
	    		ArrayList<Complex[]> tmp =	Output.get(entry);
	    		if(tmp == null) {
	    			tmp = new ArrayList<Complex[]>();
	    		}
	    		tmp.add(ncoef);
	    		Output.put(entry, tmp);
	    	}
	    }
		return Output;*/
	 return null;}

}
