package infore.SDE.synopses;
import com.fasterxml.jackson.databind.JsonNode;
import org.streaminer.stream.frequency.AMSSketch;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

public class AMSsynopsis extends Synopsis{
	private AMSSketch ams;
	
	
	public AMSsynopsis(int uid,String[] parameters) {
		super(uid,parameters[0],parameters[1],parameters[2]);
		ams = new AMSSketch(Integer.parseInt(parameters[3]), Integer.parseInt(parameters[4]));
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
		String key = node.get(this.keyIndex).asText();
		String value = node.get(this.valueIndex).asText();
		ams.add((long) Math.abs(key.hashCode()),(long)Double.parseDouble(value));
		
	}


	@Override
	public String estimate(Object k) {
		// TODO Auto-generated method stub
		return Long.toString(ams.estimateCount((long)k));
	}

	@Override
	public Synopsis merge(Synopsis sk) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public Estimation estimate(Request rq) {
	try {
		return new Estimation(rq, Double.toString((double)ams.estimateCount((long)Math.abs(rq.getParam()[0].hashCode()))), Integer.toString(rq.getUID()));
	}catch(Exception e){
		return new Estimation(rq, null, Integer.toString(rq.getUID()));
	}
	}

}
