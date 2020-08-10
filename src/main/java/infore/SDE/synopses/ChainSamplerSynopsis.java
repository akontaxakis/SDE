package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.streaminer.stream.sampler.ChainSampler;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.io.IOException;

public class ChainSamplerSynopsis extends Synopsis {
	
	ChainSampler cs;

	public ChainSamplerSynopsis(int uid, String[] parameters) {
	     super(uid,parameters[0],parameters[1]);		
	     cs = new ChainSampler(Integer.parseInt(parameters[2]),Integer.parseInt(parameters[3]));
		}
		 
		@Override
		public void add(Object k) {
			String j = (String)k;
			ObjectMapper mapper = new ObjectMapper();
			JsonNode node = null;
			try {
				node = mapper.readTree(j);
			} catch (IOException e) {
				e.printStackTrace();
			}
			String value = node.get(this.valueIndex).asText();
			cs.sample((long)Double.parseDouble(value));
		}

		@Override
		public Object estimate(Object k) {
			return cs.getSamples();		
		}

		public Estimation estimate(Request rq) {
			rq.setNoOfP(1);
			return new Estimation(rq, cs.getSamples(), Integer.toString(rq.getUID()));
		}
		
		
		
		
		
		@Override
		public Synopsis merge(Synopsis sk) {
			return sk;
			
		}
		
		
		
		
	}
