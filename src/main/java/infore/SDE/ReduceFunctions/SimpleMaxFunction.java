package infore.SDE.ReduceFunctions;


public class SimpleMaxFunction extends ReduceFunction{

	public SimpleMaxFunction(int nOfP, int count, String[] parameters, int Syn) {
		super(nOfP, count, parameters,Syn);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object reduce() {
		long max = 0;
		
		for ( Object entry : this.getEstimations()) {
		  
			if(max <(long)entry)
			   max = (long)entry;
		}

		
		return max;
	}

}
