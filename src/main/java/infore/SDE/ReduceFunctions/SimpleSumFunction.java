package infore.SDE.ReduceFunctions;


public class SimpleSumFunction extends ReduceFunction {

	public SimpleSumFunction(int nOfP, int count, String[] parameters, int syn) {
		super(nOfP, count, parameters, syn);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object reduce() {
		double sum = 0;
		
		for (Object entry : this.getEstimations()) {
			sum = sum + Double.parseDouble((String)entry);
		}

		
		return sum;
	}

}