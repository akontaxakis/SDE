package infore.SDE.transformations;

import java.io.Serializable;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import infore.SDE.messages.Request;

public class RqRouterFlatMap extends RichFlatMapFunction<Request, Request> implements Serializable{

	
	private static final long serialVersionUID = 1L;

   //SourceID + Uid (1-1000),Keys(1-1000),        
	@Override
	public void flatMap(Request rq, Collector<Request> out) throws Exception {
		 
			 //ADD-REMOVE-ESTIMATE SKETCH FOR A STREAM
			  if(rq.getRequestID()%10 == 1 || rq.getRequestID()%10 == 2 || rq.getRequestID()%10 ==3) {
				  if(rq.getNoOfP() == 1)
				  out.collect(rq);
				  else {
					  String tmpkey = ""+rq.getKey();
					  for(int i = 0; i < rq.getNoOfP(); i ++) {
						  rq.setKey(tmpkey + "_" + i);
						  out.collect(rq);
					  }
				  } 	
			  }	  
	}
}
