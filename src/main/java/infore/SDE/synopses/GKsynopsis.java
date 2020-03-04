package infore.SDE.synopses;
import org.streaminer.stream.quantile.GKQuantiles;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;


public class GKsynopsis extends Synopsis {

	GKQuantiles gk;
	
	/**
     * Creates a new GKQuantiles object that computes epsilon-approximate quantiles.
     *  
     * @param epsilon The maximum error bound for quantile estimation.
     */
	public GKsynopsis(int uid,String[] parameters) {
	     super(uid,parameters[0],parameters[1]);		
	     gk = new GKQuantiles(Double.parseDouble(parameters[2]));
		}
		 
		@Override
		public void add(Object k) {
			String j = (String)k;
			String[] tokens = j.split(",");
			gk.offer(Double.parseDouble(tokens[this.valueIndex]));
		}
		@Override
		public Object estimate(Object k) {
			return gk.getQuantile((double)k);		
		}

		@Override
		public Estimation estimate(Request rq) {

			return new Estimation(rq, gk.getQuantile(Double.parseDouble(rq.getParam()[0])), Integer.toString(rq.getUID()));
		}
		
		
		
		@Override
		public Synopsis merge(Synopsis sk) {
			return sk;
			
		}
		
		
		
		
	}

    

