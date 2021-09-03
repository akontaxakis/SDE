package infore.SDE.reduceFunctions;
import infore.SDE.messages.Estimation;

abstract public class ReduceFunction {
	protected int nOfP;
	protected int count;
	protected String[] parameters;
	protected int SynopsisID;
	protected int requestID;

	public ReduceFunction() {

	}

	abstract public Object reduce();
	abstract public boolean add(Estimation e);

	public ReduceFunction(int nOfP, int count, String[] parameters, int synID, int rqid) {
		this.requestID = rqid;
		this.SynopsisID = synID;
		this.nOfP = nOfP;
		this.count = count;
		this.parameters = parameters;

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
