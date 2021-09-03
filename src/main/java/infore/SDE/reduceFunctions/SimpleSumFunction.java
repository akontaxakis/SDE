package infore.SDE.reduceFunctions;


import infore.SDE.messages.Estimation;

import java.util.ArrayList;

public class SimpleSumFunction extends ReduceFunction {

	private ArrayList<Object> estimations;

	public SimpleSumFunction(int nOfP, int count, String[] parameters, int syn, int rq) {
		super(nOfP, count, parameters, syn, rq);
		estimations = new ArrayList<>();
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object reduce() {
		double sum = 0;
		
		for (Object entry : estimations) {
			sum = sum + Double.parseDouble((String)entry);
		}

		
		return sum;
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
