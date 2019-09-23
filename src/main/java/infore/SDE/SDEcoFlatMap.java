package infore.SDE;

import java.io.File;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import infore.SDE.sketches.AMSsketch;
import infore.SDE.sketches.Bloomfilter;
import infore.SDE.sketches.CountMin;
import infore.SDE.sketches.Sketch;
import infore.SDE.sketches.TimeSeriesSketch;

public class SDEcoFlatMap
		extends RichCoFlatMapFunction<Tuple2<Integer, Double>, Tuple2<Integer, String>, Tuple2<Integer, Object>> {

	private static final long serialVersionUID = 1L;
	private static HashMap<Integer, ArrayList<Sketch>> Sketches = new HashMap<Integer, ArrayList<Sketch>>();
	public static ArrayList<String> distKeys = new ArrayList<>();
	private static int counter = 0;

	@Override
	public void flatMap1(Tuple2<Integer, Double> node, Collector<Tuple2<Integer, Object>> collector) throws Exception {

		ArrayList<Sketch> sk = Sketches.get(node.f0);

		if (sk != null) {
			for (Sketch ski : sk) {
				ski.add(node.f1);
			}

		}
	}

	@Override
	public void flatMap2(Tuple2<Integer, String> node, Collector<Tuple2<Integer, Object>> collector) throws Exception {

		String[] tokens = node.f1.split(",");
		tokens[0] = tokens[0].replace("\"", "");
		tokens[1] = tokens[1].replace("\"", "");

		int v = Integer.parseInt(tokens[0]); // value
		int q = Integer.parseInt(tokens[1]); // 1->countMin /, 2= Bloomfilter /3-> AMS sketch //4-> TimeSeries-DFT
												// /5-> LSH / 6-> dynamic load sketch // queries other -> queryID /

		// Count min
		if (q == 1) {
			CountMin sketch;
			if (tokens.length == 5)
				sketch = new CountMin(Double.parseDouble(tokens[2]), Double.parseDouble(tokens[3]),
						Integer.parseInt(tokens[4]));
			else
				sketch = new CountMin(0.0002, 0.99, 4);
			ArrayList<Sketch> sk = Sketches.get(node.f0);
			if (sk == null) {
				sk = new ArrayList<Sketch>();
			}
			sk.add(sketch);
			Sketches.put(node.f0, sk);
			counter++;

			// BloomFliter
		} else if (q == 2) {
			Bloomfilter sketch;
			if (tokens.length == 4)
				sketch = new Bloomfilter(Integer.parseInt(tokens[2]), Double.parseDouble(tokens[3]));
			else
				sketch = new Bloomfilter(100000, 0.0002);
			ArrayList<Sketch> sk = Sketches.get(node.f0);
			if (sk == null) {
				sk = new ArrayList<Sketch>();
			}
			sk.add(sketch);
			Sketches.put(node.f0, sk);

			counter++;

			// AMS sketch
		} else if (q == 3) {
			AMSsketch sketch;
			if (tokens.length == 4)
				sketch = new AMSsketch(Integer.parseInt(tokens[2]), Integer.parseInt(tokens[3]));
			else
				sketch = new AMSsketch(100, 10);
			ArrayList<Sketch> sk = Sketches.get(node.f0);
			if (sk == null) {
				sk = new ArrayList<Sketch>();
			}
			sk.add(sketch);
			Sketches.put(node.f0, sk);
			counter++;

			// TimeSeries sketch
		} else if (q == 4) {

			TimeSeriesSketch sketch;
			if (tokens.length == 3)
				sketch = new TimeSeriesSketch(node.f0.toString(),Integer.parseInt(tokens[2]));
			else	
				sketch = new TimeSeriesSketch(node.f0.toString(),30);
			
			if (Sketches.get(node.f0) != null) {
				ArrayList<Sketch> sk = Sketches.get(node.f0);
				sk = new ArrayList<Sketch>();
				sk.add(sketch);
				Sketches.put(node.f0, sk);

			} else {

				ArrayList<Sketch> sk = new ArrayList<Sketch>();
				sk.add(sketch);
				Sketches.put(node.f0, sk);

			}

			// 5-> LSH - unfinished
		} else if (q == 5) {

			Bloomfilter sketch = new Bloomfilter(100000, 0.0002);
			ArrayList<Sketch> sk = Sketches.get(node.f0);
			if (sk == null) {
				sk = new ArrayList<Sketch>();
			}
			sk.add(sketch);
			Sketches.put(node.f0, sk);

			counter++;
			
			// 6-> dynamic load sketch
		} else if (q == 6) {
			
			Object instance;
			
			if (tokens.length == 4) {
				
			File myJar = new File(tokens[2]);
			URLClassLoader child = new URLClassLoader(new URL[] { myJar.toURI().toURL() },
			this.getClass().getClassLoader());
			Class<?> classToLoad = Class.forName(tokens[3], true, child);
			instance = classToLoad.getConstructor().newInstance();
			
			}
			else{
			
			File myJar = new File("C:\\Users\\ado.kontax\\Desktop\\flinkSketches.jar");
			URLClassLoader child = new URLClassLoader(new URL[] { myJar.toURI().toURL() },
			this.getClass().getClassLoader());
			Class<?> classToLoad = Class.forName("com.yahoo.sketches.sampling.NewSketch", true, child);
			instance = classToLoad.getConstructor().newInstance();
								
			}
				
			ArrayList<Sketch> sk = Sketches.get(node.f0);
			if (sk == null) {
				sk = new ArrayList<Sketch>();
			}
			sk.add((Sketch) instance);
			Sketches.put(node.f0, sk);

			counter++;
			System.out.println(counter);

		}
        
		//Estimate
		else {
			ArrayList<Sketch> sketches = Sketches.get(node.f0);

			if (sketches != null) {
				for (Sketch sk : sketches) {
					String j = (String) sk.estimate(v);
					assert (j.equals(null));
					collector.collect(new Tuple2<>(q,(Object)(node.f0 + " " + j)));

				}

			}

		}

	}

	public void open(Configuration config) throws Exception {

		MapStateDescriptor<Integer, ArrayList<Sketch>> descriptor = new MapStateDescriptor<>("request",
				TypeInformation.of(new TypeHint<Integer>() {
				}), TypeInformation.of(new TypeHint<ArrayList<Sketch>>() {
				}));

		descriptor.setQueryable("request");

	}

}
