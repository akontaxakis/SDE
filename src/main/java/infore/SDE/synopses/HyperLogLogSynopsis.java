package infore.SDE.synopses;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.streaminer.stream.cardinality.HyperLogLog;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.io.IOException;

public class HyperLogLogSynopsis extends Synopsis {
	
	HyperLogLog hll;
	
	public HyperLogLogSynopsis(int uid, String[] parameters) {
	     super(uid, parameters[0],parameters[1],parameters[2]);
	     hll = new HyperLogLog(Double.parseDouble(parameters[3]));
		}
		 
		@Override
		public void add(Object k) {
			//ObjectMapper mapper = new ObjectMapper();
			JsonNode node = (JsonNode)k;
        /*try {
            node = mapper.readTree(j);
        } catch (IOException e) {
            e.printStackTrace();
        } */
			String value = node.get(this.valueIndex).asText();
			hll.offer(value);
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

