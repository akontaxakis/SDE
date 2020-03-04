package infore.SDE.synopses;

import java.io.Serializable;

import org.streaminer.stream.frequency.AMSSketch;

import com.clearspring.analytics.stream.frequency.CountMinSketch;



public class DeliverableCM  implements Serializable{
	 transient private AMSSketch cm;
	
	private static final long serialVersionUID = 1L;

	public DeliverableCM() {
	
		cm = new AMSSketch(5,20);
	}
	


@SuppressWarnings("deprecation")
public void add(String j,double k) {
	
	cm.add(new Double(k).longValue());


}
}

