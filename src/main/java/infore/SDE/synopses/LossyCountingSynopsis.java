package infore.SDE.synopses;
import org.streaminer.stream.frequency.LossyCounting;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;


public class LossyCountingSynopsis extends Synopsis {
	
	LossyCounting<String> sk;
	
	public LossyCountingSynopsis(int uid, String[] parameters) {
	     super(uid, parameters[0],parameters[1]);		
		 sk = new LossyCounting<String>(Double.parseDouble(parameters[2]));
		}
		 
		@SuppressWarnings("unchecked")
		@Override
		public void add(Object k) {
			String j = (String)k;
			String[] tokens = j.split(",");
			sk.add((tokens[this.keyIndex]),(long)Double.parseDouble(tokens[this.valueIndex]));
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object estimate(Object k) {
			return sk.estimateCount((String)k);
		}
		
		@Override
		public Estimation estimate(Request rq) {

			return new Estimation(rq,  Double.toString((double)sk.estimateCount(rq.getParam()[0])), Integer.toString(rq.getUID()));
		}
		
		
		
		
		@Override
		public Synopsis merge(Synopsis sk) {
			return sk;
			
		}
		
		
		
		
	}
