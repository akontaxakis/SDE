package infore.SDE.synopses;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.sketches.TimeSeries.COEF;
import org.apache.commons.math3.complex.Complex;


public class MultySynopsisDFT extends Synopsis{
	private  HashMap<String, DFT> Synopses;
	String[] parameters;
	
	public MultySynopsisDFT(int uid, String[] param) {
		super(uid, param[0], param[1]);
		Synopses = new HashMap<String, DFT>();
		parameters = param;
		// TODO Auto-generated constructor stub
	}

	@Override
	public void add(Object k) {
		String j = (String)k;
		// TODO Auto-generated method stub

		ObjectMapper mapper = new ObjectMapper();
		JsonNode node = null;
		try {
			node = mapper.readTree(j);
		} catch (IOException e) {
			e.printStackTrace();
		}
		String key = node.get(this.keyIndex).asText();
		DFT dft = Synopses.get(key);
		if(dft == null)
			dft = new DFT(this.SynopsisID,parameters,key);

		dft.add(j);
		Synopses.put(key, dft);
		
	}

	@Override
	public Object estimate(Object k) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Synopsis merge(Synopsis sk) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Estimation estimate(Request rq) {

		 ArrayList<COEF> Output = new ArrayList<>();

		for( Synopsis pair:  Synopses.values()) {
			DFT df = (DFT) pair;
			Output.add(df.getCOEF());
			}

			return new Estimation(rq, Output, Integer.toString(rq.getUID()));
	}


	public HashMap<String, ArrayList<COEF>> estimate2(Request rq) {

		HashMap<String, ArrayList<COEF>> Output = new HashMap<>();
		// TODO Auto-generated method stub
		//Iterator<Entry<String, Synopsis>> it = Synopses.entrySet().iterator();
		//System.out.println("Size = " + Synopses.size());
		double epsilon = Math.sqrt(1 - Double.parseDouble(rq.getParam()[0]));
		int hashOffset = (int) Math.floor(Math.sqrt(2) /(2 *(epsilon)));

		for(Map.Entry<String, DFT> pair:  Synopses.entrySet()) {

			DFT df =  pair.getValue();
			COEF ncoef = df.getCOEF();

			ArrayList<String> str = df.getKeys2(Double.parseDouble(rq.getParam()[0]));


			for(String entry: str) {

				ArrayList<COEF> tmp = Output.get("Bucket_"+entry);
				if(tmp == null) {
					tmp = new ArrayList<COEF>();
				}
				tmp.add(ncoef);
				Output.put("Bucket_"+entry, tmp);
			}
		}
		int c=0;
		ArrayList<COEF> tmp = new ArrayList<>();
		tmp.add(new COEF("init",null));
		for(int i =0; i <hashOffset*2*hashOffset*2;i++ ){
			if(Output.get("Bucket_"+i)==null){
				Output.put("Bucket_"+i,tmp);
				c++;
			}

		}

		return Output;
	}



}
