package infore.SDE.ReduceFunctions;


public class SimpleAvgFunction extends ReduceFunction {

	public SimpleAvgFunction(int nOfP, int count, String[] parameters, int syn, int rq) {
		super(nOfP, count, parameters, syn, rq);
		
	}

	@Override
	public Object reduce() {
		long avg = 0;
		
		for ( Object entry : this.getEstimations()) {
			avg = avg + (long)entry;
		}

		
		return avg/this.nOfP;
	}

}
