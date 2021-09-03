package infore.SDE.reduceFunctions;

import infore.SDE.messages.Estimation;

import java.util.ArrayList;

public class SimpleORFunction extends ReduceFunction {

   private ArrayList<Object> estimations;

	public SimpleORFunction(int nOfP, int count, String[] parameters, int syn, int rq) {
		super(nOfP, count, parameters, syn, rq);
		estimations = new ArrayList<>();
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object reduce() {
		boolean or = false;
		
		for (Object entry : estimations) {
			or = or || (boolean)entry;
		}
		return or;
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
