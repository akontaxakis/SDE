package infore.SDE.synopses;

import com.clearspring.analytics.stream.frequency.CountMinSketch;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

public class CountMin extends Synopsis{

	private CountMinSketch cm;

	public CountMin(int uid, String[] parameters) {
     super(uid,parameters[0],parameters[1]);		
	 cm = new CountMinSketch(Double.parseDouble(parameters[2]),Double.parseDouble(parameters[3]),Integer.parseInt(parameters[4]));
	}
	 
	@Override
	public void add(Object k) {
		String j = (String)k;
		String[] tokens = j.split(",");

		cm.add(Math.abs((tokens[this.keyIndex]).hashCode()), (long)Integer.parseInt(tokens[this.valueIndex]));
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
		return new Estimation(rq, Math.abs(cm.estimateCount(rq.getParam()[0].hashCode())), Integer.toString(rq.getUID()));
	}
	
	
	
	
}
