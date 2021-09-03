package infore.SDE.synopses;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.streaminer.stream.frequency.LossyCounting;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.io.IOException;


public class LossyCountingSynopsis extends Synopsis {
	
	LossyCounting<String> sk;
	
	public LossyCountingSynopsis(int uid, String[] parameters) {
	     super(uid, parameters[0],parameters[1],parameters[2]);
		 sk = new LossyCounting<String>(Double.parseDouble(parameters[3]));
		}
		 
		@SuppressWarnings("unchecked")
		@Override
		public void add(Object k) {
			//ObjectMapper mapper = new ObjectMapper();
			JsonNode node = (JsonNode)k;
        /*try {
            node = mapper.readTree(j);
        } catch (IOException e) {
            e.printStackTrace();
        } */
			String key = node.get(this.keyIndex).asText();
			String value = node.get(this.valueIndex).asText();
			sk.add(key,(long)Double.parseDouble(value));
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
