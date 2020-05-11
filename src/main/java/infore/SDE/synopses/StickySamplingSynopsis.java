package infore.SDE.synopses;
import org.streaminer.stream.frequency.StickySampling;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

public class StickySamplingSynopsis extends Synopsis {
	
	StickySampling<String> sk;

	public StickySamplingSynopsis(int uid, String[] parameters) {
	     super(uid, parameters[0],parameters[1]);		
		 sk = new StickySampling<String>(Double.parseDouble(parameters[2]),Double.parseDouble(parameters[3]),Double.parseDouble(parameters[4]));
		}
		 
		@Override
		public void add(Object k) {
			String j = (String)k;
			String[] tokens = j.split(",");
			sk.add(tokens[this.keyIndex],(long)Double.parseDouble(tokens[this.valueIndex]));
		}

		@Override
		public Object estimate(Object k) {
			return sk.estimateCount((String) k);
		}

		@Override
		public Synopsis merge(Synopsis sk) {
			return sk;
			
		}

		@Override
		public Estimation estimate(Request rq) {
			// TODO Auto-generated method stub
			return new Estimation(rq, Double.toString((double)sk.estimateCount(rq.getParam()[0])), Integer.toString(rq.getUID()));
		}

	}
