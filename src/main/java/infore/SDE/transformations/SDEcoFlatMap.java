package infore.SDE.transformations;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.complex.Complex;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.synopses.AMSsynopsis;
import infore.SDE.synopses.Bloomfilter;
import infore.SDE.synopses.ChainSamplerSynopsis;
import infore.SDE.synopses.Coresets;
import infore.SDE.synopses.CountMin;
import infore.SDE.synopses.Counters;
import infore.SDE.synopses.DFT;
import infore.SDE.synopses.FinJoinSynopsis;
import infore.SDE.synopses.GKsynopsis;
import infore.SDE.synopses.HyperLogLogSynopsis;
import infore.SDE.synopses.LossyCountingSynopsis;
import infore.SDE.synopses.StickySamplingSynopsis;
import infore.SDE.synopses.Synopsis;

public class SDEcoFlatMap extends RichCoFlatMapFunction<Tuple2<String, String>, Request, Estimation> {

	private static final long serialVersionUID = 1L;
	private ArrayList<Synopsis> Synopses = new ArrayList<Synopsis>();
	int pId;

	@Override
	public void flatMap1(Tuple2<String, String> node, Collector<Estimation> collector) throws Exception {

		String value = node.f1.toString().replace("\"", "");

		if (Synopses != null) {
			for (Synopsis ski : Synopses) {
				ski.add(value);
			}
		}
	}

	@Override
	public void flatMap2(Request rq, Collector<Estimation> collector) throws Exception {
		// String key = rq.getKey();

		System.out.println(rq.toString());

		if (rq.getRequestID() == 1) {
			// countMin
			if (rq.getSynopsisID() == 1) {
				CountMin sketch;
				if (rq.getParam().length > 4)
					sketch = new CountMin(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "1", "0.0002", "0.99", "4" };
					sketch = new CountMin(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);

				// BloomFliter
			} else if (rq.getSynopsisID() == 2) {
				Bloomfilter sketch;
				if (rq.getParam().length > 3)
					sketch = new Bloomfilter(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "1", "100000", "0.0002" };
					sketch = new Bloomfilter(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);

				// AMS sketch
			} else if (rq.getSynopsisID() == 3) {
				AMSsynopsis sketch;
				if (rq.getParam().length > 3)
					sketch = new AMSsynopsis(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "1", "100", "10" };
					sketch = new AMSsynopsis(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);

				// TimeSeries sketch
			} else if (rq.getSynopsisID() == 4) {
				DFT sketch;

				if (rq.getParam().length > 4)
					sketch = new DFT(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "1", "15", "1", "1" };
					sketch = new DFT(rq.getUID(), _tmp);
				}

				Synopses.add(sketch);
				// 5-> LSH - unfinished
			} else if (rq.getSynopsisID() == 5) {

				Bloomfilter sketch = new Bloomfilter(rq.getUID(), rq.getParam());
				Synopses.add(sketch);

				// Coresets
			} else if (rq.getSynopsisID() == 6) {
				Coresets sketch;

				if (rq.getParam().length > 3)
					sketch = new Coresets(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "5", "15", "1", "1" };
					sketch = new Coresets(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);
				// HyperLogLog
			} else if (rq.getSynopsisID() == 7) {
				HyperLogLogSynopsis sketch;
				if (rq.getParam().length > 3)
					sketch = new HyperLogLogSynopsis(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "1", "100", "10" };
					sketch = new HyperLogLogSynopsis(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);

				// StickySampling
			} else if (rq.getSynopsisID() == 8) {
				StickySamplingSynopsis sketch;
				if (rq.getParam().length > 3)
					sketch = new StickySamplingSynopsis(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "1", "100", "10" };
					sketch = new StickySamplingSynopsis(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);

				// LossyCounting
			} else if (rq.getSynopsisID() == 9) {
				LossyCountingSynopsis sketch;
				if (rq.getParam().length > 3)
					sketch = new LossyCountingSynopsis(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "1", "100", "10" };
					sketch = new LossyCountingSynopsis(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);

				// ChainSampler
			} else if (rq.getSynopsisID() == 10) {
				ChainSamplerSynopsis sketch;
				if (rq.getParam().length > 3)
					sketch = new ChainSamplerSynopsis(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "1", "100", "10" };
					sketch = new ChainSamplerSynopsis(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);

				// GKsynopsis
			} else if (rq.getSynopsisID() == 11) {
				GKsynopsis sketch;
				if (rq.getParam().length > 3)
					sketch = new GKsynopsis(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "1", "1", "100", "10" };
					sketch = new GKsynopsis(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);

				// 6-> dynamic load sketch
			} else if (rq.getSynopsisID() == 12) {

				Object instance;

				if (rq.getParam().length == 4) {

					File myJar = new File(rq.getParam()[2]);
					URLClassLoader child = new URLClassLoader(new URL[] { myJar.toURI().toURL() },
							this.getClass().getClassLoader());
					Class<?> classToLoad = Class.forName(rq.getParam()[3], true, child);
					instance = classToLoad.getConstructor().newInstance();

				} else {

					File myJar = new File("C:\\Users\\ado.kontax\\Desktop\\flinkSketches.jar");
					URLClassLoader child = new URLClassLoader(new URL[] { myJar.toURI().toURL() },
							this.getClass().getClassLoader());
					Class<?> classToLoad = Class.forName("com.yahoo.sketches.sampling.NewSketch", true, child);
					instance = classToLoad.getConstructor().newInstance();
					Synopses.add((Synopsis) instance);

				}
				// FINJOIN
			} else if (rq.getSynopsisID() == 13) {

				FinJoinSynopsis sketch;

				if (rq.getParam().length > 3)
					sketch = new FinJoinSynopsis(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "0", "0", "10", "100", "8", "3" };
					sketch = new FinJoinSynopsis(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);
				// COUNT
			} else if (rq.getSynopsisID() == 14) {

				Counters sketch;

				if (rq.getParam().length > 3)
					sketch = new Counters(rq.getUID(), rq.getParam());
				else {
					String[] _tmp = { "0", "0", "10", "100", "8", "3" };
					sketch = new Counters(rq.getUID(), _tmp);
				}
				Synopses.add(sketch);
			}

		}
		// Estimate - delete
		else {

			for (Synopsis syn : Synopses) {

				if (rq.getUID() == syn.getSynopsisID()) {
					if (rq.getRequestID() / 10 == 2) {
						Synopses.remove(syn);
						break;
					} else {
						if (rq.getSynopsisID() == 13) {
							FinJoinSynopsis fj = (FinJoinSynopsis) syn;
							HashMap<String, ArrayList<Complex[]>> out = fj.estimate2(rq);
							for (Map.Entry<String, ArrayList<Complex[]>> pair : out.entrySet()) {
								collector.collect(new Estimation(rq, pair.getValue(), pair.getKey()));
							}
							break;
						}else {

						Estimation e = syn.estimate(rq);
						collector.collect(e);
						break;
						}
					}
				}
			}
		}
	}

	public void open(Configuration config) throws Exception {
		pId = getRuntimeContext().getIndexOfThisSubtask();
		MapStateDescriptor<Integer, ArrayList<Synopsis>> descriptor = new MapStateDescriptor<>("request",
				TypeInformation.of(new TypeHint<Integer>() {
				}), TypeInformation.of(new TypeHint<ArrayList<Synopsis>>() {
				}));
		descriptor.setQueryable("request");
	}

}
