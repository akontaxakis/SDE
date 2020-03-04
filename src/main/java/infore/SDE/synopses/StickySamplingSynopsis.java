package infore.SDE.synopses;
import org.streaminer.stream.frequency.StickySampling;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

public class StickySamplingSynopsis extends Synopsis {
	
	StickySampling<Double> sk;

	public StickySamplingSynopsis(int uid, String[] parameters) {
	     super(uid, parameters[0],parameters[1]);		
		 sk = new StickySampling<Double>(Double.parseDouble(parameters[2]),Double.parseDouble(parameters[3]),Double.parseDouble(parameters[4]));
		}
		 
		@Override
		public void add(Object k) {
			String j = (String)k;
			String[] tokens = j.split(",");
			sk.add(Double.parseDouble(tokens[this.keyIndex]),Long.parseLong(tokens[this.valueIndex]));	
		}

		@Override
		public Object estimate(Object k) {
			return sk.estimateCount((double)k);		
		}

		@Override
		public Synopsis merge(Synopsis sk) {
			return sk;
			
		}

		@Override
		public Estimation estimate(Request rq) {
			// TODO Auto-generated method stub
			return new Estimation(rq, sk.estimateCount(Double.parseDouble(rq.getParam()[0])), Integer.toString(rq.getUID()));
		}
		
		
		
		
	}
