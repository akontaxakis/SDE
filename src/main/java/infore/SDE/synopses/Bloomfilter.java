package infore.SDE.synopses;

import com.clearspring.analytics.stream.membership.BloomFilter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.io.IOException;

public class Bloomfilter extends Synopsis{
 private BloomFilter bm;
 
 public Bloomfilter(int uid, String[] parameters) {
	 super(uid,parameters[0],parameters[1]);
	 bm = new BloomFilter( Integer.parseInt(parameters[2]), Double.parseDouble(parameters[3]));
	 
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
		String key = node.get(this.keyIndex).asText();
		bm.add(key);
		
	}
	
	@Override
	public String estimate(Object k) {
		 if(bm.isPresent((Double.toString((double)k))))
		return "1";
		return "0";

	}

	public Estimation estimate(Request rq) {

		return new Estimation(rq, bm.isPresent(rq.getParam()[0]), Integer.toString(rq.getUID()));
	}



	@Override
	public Synopsis merge(Synopsis sk) {
		// TODO Auto-generated method stub
		return sk;
	}
 
}
