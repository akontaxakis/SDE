package infore.SDE.synopses;
import org.streaminer.stream.cardinality.HyperLogLog;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

public class HyperLogLogSynopsis extends Synopsis {
	
	HyperLogLog hll;
	
	public HyperLogLogSynopsis(int uid, String[] parameters) {
	     super(uid, parameters[0],parameters[1]);		
	     hll = new HyperLogLog(Double.parseDouble(parameters[2]));
		}
		 
		@Override
		public void add(Object k) {
			String j = (String)k;
			String[] tokens = j.split(",");
			hll.offer(tokens[this.valueIndex]);	
		}

		@Override
		public Object estimate(Object k) {
			return hll.cardinality();		
		}

		@Override
		public Estimation estimate(Request rq) {

			return new Estimation(rq, Double.toString((double)hll.cardinality()), Integer.toString(rq.getUID()));
		}
		
		
		@Override
		public Synopsis merge(Synopsis sk) {
			return sk;
			
		}
		
		
		
		
	}

