package infore.SDE.reduceFunctions;


import infore.SDE.messages.Estimation;

import java.util.ArrayList;

public class SimpleAvgFunction extends ReduceFunction {

	private ArrayList<Object> estimations;

	public SimpleAvgFunction(int nOfP, int count, String[] parameters, int syn, int rq) {
		super(nOfP, count, parameters, syn, rq);
		estimations = new ArrayList<>();

	}

	@Override
	public Object reduce() {
		long avg = 0;
		
		for ( Object entry : estimations) {
			avg = avg + (long)entry;
		}

		
		return avg/this.nOfP;
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
