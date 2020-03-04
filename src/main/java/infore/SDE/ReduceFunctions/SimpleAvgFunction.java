package infore.SDE.ReduceFunctions;


public class SimpleAvgFunction extends ReduceFunction {

	public SimpleAvgFunction(int nOfP, int count, String[] parameters, int syn) {
		super(nOfP, count, parameters, syn);
		
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
