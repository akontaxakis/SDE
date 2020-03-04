package infore.SDE.synopses;
import org.streaminer.stream.frequency.LossyCounting;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;


public class LossyCountingSynopsis extends Synopsis {
	
	LossyCounting<Double> sk;
	
	public LossyCountingSynopsis(int uid, String[] parameters) {
	     super(uid, parameters[0],parameters[1]);		
		 sk = new LossyCounting<Double>(Double.parseDouble(parameters[2]));
		}
		 
		@SuppressWarnings("unchecked")
		@Override
		public void add(Object k) {
			String j = (String)k;
			String[] tokens = j.split(",");
			sk.add((Double.parseDouble(tokens[this.keyIndex])),Long.parseLong(tokens[this.valueIndex]));	
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object estimate(Object k) {
			return sk.estimateCount((Double)k);		
		}
		
		@Override
		public Estimation estimate(Request rq) {

			return new Estimation(rq,  sk.estimateCount(Double.parseDouble(rq.getParam()[0])), Integer.toString(rq.getUID()));
		}
		
		
		
		
		@Override
		public Synopsis merge(Synopsis sk) {
			return sk;
			
		}
		
		
		
		
	}
