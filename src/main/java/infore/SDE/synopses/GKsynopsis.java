package infore.SDE.synopses;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.streaminer.stream.quantile.GKQuantiles;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.io.IOException;


public class GKsynopsis extends Synopsis {

	GKQuantiles gk;
	
	/**
     * Creates a new GKQuantiles object that computes epsilon-approximate quantiles.
     *  
     * @param uid The maximum error bound for quantile estimation.
     */
	public GKsynopsis(int uid,String[] parameters) {
	     super(uid,parameters[0],parameters[1],parameters[2]);
	     gk = new GKQuantiles(Double.parseDouble(parameters[3]));
		 gk.setEpsilon(Double.parseDouble(parameters[4]));
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
			gk.offer(Double.parseDouble(value) % 10);
		}
		@Override
		public Object estimate(Object k) {
			return gk.getQuantile((double)k);		
		}
		@Override
		public Estimation estimate(Request rq) {


			try {
				return new Estimation(rq, gk.getQuantile(Double.parseDouble(rq.getParam()[0])), Integer.toString(rq.getUID()));
			}catch(Exception e){
				return new Estimation(rq, null, Integer.toString(rq.getUID()));
			}



		}
		@Override
		public Synopsis merge(Synopsis sk) {
			return sk;
			
		}
		
		
		
		
	}

    

