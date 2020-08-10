package infore.SDE.synopses;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.streaminer.stream.frequency.StickySampling;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.io.IOException;

public class StickySamplingSynopsis extends Synopsis {
	
	StickySampling<String> sk;

	public StickySamplingSynopsis(int uid, String[] parameters) {
	     super(uid, parameters[0],parameters[1]);		
		 sk = new StickySampling<String>(Double.parseDouble(parameters[2]),Double.parseDouble(parameters[3]),Double.parseDouble(parameters[4]));
		}
		 
		@Override
		public void add(Object k) {
			String j = (String)k;
			// TODO Auto-generated method stub

			ObjectMapper mapper = new ObjectMapper();
			JsonNode node = null;
			try {
				node = mapper.readTree(j);
			} catch (IOException e) {
				e.printStackTrace();
			}
			String key = node.get(this.keyIndex).asText();
			String value = node.get(this.valueIndex).asText();
			sk.add(key,(long)Double.parseDouble(value));
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
