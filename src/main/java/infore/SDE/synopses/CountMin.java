package infore.SDE.synopses;



import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.io.IOException;

public class CountMin extends Synopsis{

	private CM cm;

	public CountMin(int uid, String[] parameters) {
     super(uid,parameters[0],parameters[1], parameters[2]);
	 cm = new CM(Double.parseDouble(parameters[3]),Double.parseDouble(parameters[4]),Integer.parseInt(parameters[5]));
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
		cm.add(Math.abs((key).hashCode()), (long)Double.parseDouble(value));

	}

	@SuppressWarnings("deprecation")
	@Override
	public Object estimate(Object k)
	{
		return Long.toString(cm.estimateCount(new Long((long) k)));
	}

	@Override
	public Synopsis merge(Synopsis sk) {
		return sk;	
	}

	@Override
	public Estimation estimate(Request rq) {

		if(rq.getRequestID() % 10 == 6){

			String[] par = rq.getParam();
			par[2]= ""+rq.getUID();
			rq.setUID(Integer.parseInt(par[1]));
			rq.setParam(par);
			rq.setNoOfP(rq.getNoOfP()*Integer.parseInt(par[0]));
			return new Estimation(rq, cm, par[1]);

		}
		return new Estimation(rq, Double.toString((double)cm.estimateCount(Math.abs(rq.getParam()[0].hashCode()))), Integer.toString(rq.getUID()));


	}
	
	
	
	
}
