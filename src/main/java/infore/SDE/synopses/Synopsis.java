package infore.SDE.synopses;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

abstract public class Synopsis {
	
	protected int SynopsisID;


	protected String keyIndex;


	protected String valueIndex;
	
	public Synopsis(int ID, String k, String v) {
		 SynopsisID=ID;
	     keyIndex=k;
	     valueIndex=v;
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
	public String getKeyIndex() {return keyIndex;}
	public void setKeyIndex(String keyIndex) {this.keyIndex = keyIndex;}
	public String getValueIndex() {return valueIndex;}

	public void setValueIndex(String valueIndex) {this.valueIndex = valueIndex;}

}
