package infore.SDE.synopses;

import org.streaminer.stream.sampler.ChainSampler;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

public class ChainSamplerSynopsis extends Synopsis {
	
	ChainSampler cs;

	public ChainSamplerSynopsis(int uid, String[] parameters) {
	     super(uid,parameters[0],parameters[1]);		
	     cs = new ChainSampler(Integer.parseInt(parameters[2]),Integer.parseInt(parameters[3]));
		}
		 
		@Override
		public void add(Object k) {
			String j = (String)k;
			String[] tokens = j.split(",");
			cs.sample(Long.parseLong(tokens[this.valueIndex]));	
		}

		@Override
		public Object estimate(Object k) {
			return cs.getSamples();		
		}

		public Estimation estimate(Request rq) {
			
			return new Estimation(rq, cs.getSamples(), Integer.toString(rq.getUID()));
		}
		
		
		
		
		
		@Override
		public Synopsis merge(Synopsis sk) {
			return sk;
			
		}
		
		
		
		
	}
