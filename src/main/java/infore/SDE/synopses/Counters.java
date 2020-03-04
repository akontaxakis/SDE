package infore.SDE.synopses;

import java.util.ArrayList;
import java.util.HashMap;


import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

public class Counters extends Synopsis {
	private HashMap<String, Integer> Counter;
	private HashMap<String, ArrayList<Integer>> Counters;
	private int length;

	public Counters(int uid, String[] param) {
		super(uid, param[0], param[1]);

		length = Integer.parseInt(param[3]);
		Counters = new HashMap<String, ArrayList<Integer>>();
		Counter = new HashMap<String, Integer>();
		// TODO Auto-generated constructor stub
	}

	@Override
	public void add(Object k) {
		String j = (String) k;
		// TODO Auto-generated method stub
		String[] tokens = j.split(",");
		char last = j.charAt(j.length() - 1);
		// System.out.println(last);
		if (last == '1') {

			Integer count = Counter.get(tokens[this.keyIndex]);
			if (count == null)
				count = 1;
			else
				count++;

			Counter.put(tokens[this.keyIndex], count);
			
			
			
		} else if (last == '2') {

			ArrayList<Integer> c = Counters.get(tokens[this.keyIndex]);

			if (c == null)
				c = new ArrayList<Integer>();
			
			Integer countSofar = Counter.get(tokens[this.keyIndex]);

			if (countSofar != null) {
				// System.out.println(countSofar);
				
				if(c.size() >= this.length) 
					c.remove(0);
				  
				c.add(countSofar);
				Counters.put(tokens[this.keyIndex], c);
				Counter.put(tokens[this.keyIndex], 0);
	
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
		return new Estimation(rq, Counters, Integer.toString(rq.getUID()));
	}

	

}
