package infore.SDE.ReduceFunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import infore.SDE.sketches.TimeSeries.COEF;

import infore.SDE.messages.Estimation;
import infore.SDE.synopses.CM;

abstract public  class ReduceFunction {
      
	private ArrayList<Object> estimations;
	private HashMap<String,Object> indexEstimations;
	private int nOfP;
	protected int count;
	protected String[] parameters;
	private int SynopsisID;
	private int requestID;
	abstract public Object reduce();
	

	private ReduceFunction(int nOfP, int count, String[] parameters, int synID, int rqid) {
		this.requestID = rqid;
		this.SynopsisID = synID;
		if(SynopsisID == 1 && requestID == 6){
			this.indexEstimations = new HashMap<>();
		}
		else{
			this.estimations = new ArrayList<>();
		}

		this.nOfP = nOfP;

		this.count = count;
		this.parameters = parameters;

	}
	
	
	public boolean add(Estimation e) {
		
		count++;
		int id = this.SynopsisID;
		/*if(id == 1 || id == 3 ||) {
			estimations.add(Double.parseDouble((String) e.getEstimation()));
			
		}
		else if(id == 2) {
			estimations.add(Boolean.parseBoolean((String)e.getEstimation()));
		} */
		 if(id == 4) {
			
			ArrayList<COEF> k;
			 k = (ArrayList<COEF>) e.getEstimation();
			 for(COEF c: k) {
			estimations.add(c);
			}
		}
		else if(id == 14) {
			 HashMap<String, ArrayList<Integer>> k2 = (HashMap<String, ArrayList<Integer>>) e.getEstimation();

			 for (Map.Entry<String, ArrayList<Integer>> pair : k2.entrySet()) {
				 estimations.add(pair.getValue());
			 }
		 }
		else if(SynopsisID == 1 && requestID == 6){
			 CM temp = (CM)indexEstimations.get(e.getParam()[2]);
			 if(temp ==null){
				 indexEstimations.put(e.getParam()[2], e.getEstimation());
			 }else{
				 temp.merge((CM)e.getEstimation());
				 indexEstimations.put(e.getParam()[2],temp);
			 }



		 }
		
		else {
		estimations.add(e.getEstimation());
		}
		if(count == nOfP) {
			return true;
		}
		return false;
	}
	
	
	
	private ArrayList<Object> getEstimations() {
		return estimations;
	}

	public void setEstimations(ArrayList<Object> estimations) {
		this.estimations = estimations;
	}

	public int getnOfP() {
		return nOfP;
	}

	public void setnOfP(int nOfP) {
		this.nOfP = nOfP;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String[] getParameters() {
		return parameters;
	}

	public void setParameters(String[] parameters) {
		this.parameters = parameters;
	}

	private int getSynopsisID() {
		return SynopsisID;
	}

	public void setSynopsisID(int synopsisID) {
		SynopsisID = synopsisID;
	}

	
}
