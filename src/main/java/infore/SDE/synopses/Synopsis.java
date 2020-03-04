package infore.SDE.synopses;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

abstract public class Synopsis {
	
	protected int SynopsisID;
    protected int keyIndex;
    protected int valueIndex;
	
	public Synopsis(int ID, String k, String v) {
		 SynopsisID=ID;
	     keyIndex=Integer.parseInt(k);
	     valueIndex=Integer.parseInt(v);
	}
	
	public abstract void add(Object k);
	public abstract Object estimate(Object k);
	public abstract Estimation estimate(Request rq);
	public abstract Synopsis merge(Synopsis sk);
	
    public int getSynopsisID() {
		return SynopsisID;
	}
	public void setSynopsisID(int SynopsisID) {
		this.SynopsisID = SynopsisID;
	}
	public int getKeyIndex() {
		return keyIndex;
	}
	public void setKeyIndex(int keyIndex) {
		this.keyIndex = keyIndex;
	}
	public int getValueIndex() {
		return valueIndex;
	}
	public void setValueIndex(int valueIndex) {
		this.valueIndex = valueIndex;
	}
}
