package infore.SDE.reduceFunctions;


import infore.SDE.messages.Estimation;

import java.util.ArrayList;

public class SimpleMaxFunction extends ReduceFunction{

	private ArrayList<Object> estimations;

	public SimpleMaxFunction(int nOfP, int count, String[] parameters, int Syn, int rq) {
		super(nOfP, count, parameters,Syn, rq);
		estimations = new ArrayList<>();
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object reduce() {
		long max = 0;
		
		for ( Object entry : estimations) {
		  
			if(max <(long)entry)
			   max = (long)entry;
		}

		
		return max;
	}

	@Override
	public boolean add(Estimation e) {
		estimations.add(e.getEstimation());
		count++;
		if(count == nOfP) {
		return true;
	}
		return false;
	}

}
