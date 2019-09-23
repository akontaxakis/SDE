package infore.SDE.sketches;

abstract public class Sketch {

	public abstract void add(Object k);
	public abstract Object estimate(Object k);
	public abstract Sketch merge(Sketch sk);
	
	
}
