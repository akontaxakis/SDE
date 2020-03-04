package infore.SDE.ReduceFunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.complex.Complex;

import infore.SDE.messages.Estimation;

abstract public  class ReduceFunction {
      
	protected ArrayList<Object> estimations; 
	protected int nOfP;
	protected int count;
	protected String[] parameters;
	protected int SynopsisID;
	
	abstract public Object reduce();
	

	public ReduceFunction(int nOfP, int count, String[] parameters, int synID) {
		super();
		this.estimations = new ArrayList<Object>();
		this.nOfP = nOfP;
		this.SynopsisID = synID;
		this.count = count;
		this.parameters = parameters;
	}
	
	
	public boolean add(Estimation e) {
		
		count++;
		if(this.SynopsisID == 1) {
			estimations.add(Long.parseLong((String) e.getEstimation()));
			
		}
		else if(this.SynopsisID == 13) {
			
			ArrayList<Complex[]> k = (ArrayList<Complex[]>) e.getEstimation();
			for(Complex[] c: k) {
			estimations.add(c);
			}
		}
		else if(this.SynopsisID == 14){
			
			HashMap<String, ArrayList<Integer>> k2 = (HashMap<String, ArrayList<Integer>>) e.getEstimation();
			
			for (Map.Entry<String, ArrayList<Integer>> pair : k2.entrySet()) {
					
					estimations.add(pair.getValue());
				
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
	
	
	
	public ArrayList<Object> getEstimations() {
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
	public int getSynopsisID() {
		return SynopsisID;
	}

	public void setSynopsisID(int synopsisID) {
		SynopsisID = synopsisID;
	}

	
}
