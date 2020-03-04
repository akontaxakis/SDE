package infore.SDE.synopses;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

public class LSHsynopsis extends Synopsis{
    
	private LSH lsh;
	
public LSHsynopsis(int uid, String parameters[]) {
	super(uid ,parameters[0],parameters[1]);		
	 lsh = new LSH(Double.parseDouble(parameters[2]));
}
	
	
	
	
	@Override
	public void add(Object k) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object estimate(Object k) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Synopsis merge(Synopsis sk) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Estimation estimate(Request rq) {
		// TODO Auto-generated method stub
		return null;
	}

}
