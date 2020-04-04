package infore.SDE.synopses;
import java.util.HashMap;


import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;


public class MultySynopsisDFT extends Synopsis{
	private  HashMap<String, Synopsis> Synopses;
	String[] parameters;
	
	public MultySynopsisDFT(int uid, String[] param) {
		super(uid, param[0], param[1]);
		Synopses = new HashMap<String, Synopsis>();
		parameters = param;
		// TODO Auto-generated constructor stub
	}

	@Override
	public void add(Object k) {
		String j = (String)k;
		// TODO Auto-generated method stub
		String[] tokens = j.split(",");
		
		Synopsis DFT = Synopses.get(tokens[this.keyIndex]);
		if(DFT == null)
		DFT = new DFT(this.SynopsisID,parameters);
		
		DFT.add(k);
		Synopses.put(tokens[this.keyIndex], DFT);
		
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
	   /* Iterator<Entry<String, Synopsis>> it = Synopses.entrySet().iterator();
	    while (it.hasNext()) {
	    	Entry<String, Synopsis> pair = (Entry<String, Synopsis>)it.next();
	        System.out.println(pair.getKey() + " = " + pair.getValue());
	        it.remove(); // avoids a ConcurrentModificationException
	    } */

		try {
			Synopsis DFT = Synopses.get(rq.getParam()[0]);
			return DFT.estimate(rq);
		}catch(Exception e){
			return new Estimation(rq, null, Integer.toString(rq.getUID()));
		}

	}

}
