package infore.SDE;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.sources.kafkaConsumer;
import infore.SDE.transformations.RqRouterFlatMap;
import infore.SDE.transformations.SDEcoFlatMap;
import infore.SDE.transformations.dataRouterCoFlatMap;

@SuppressWarnings("deprecation")
public class miniSDE {


	private static String kafkaDataInputTopic;
	private static String kafkaRequestInputTopic;
	private static String kafkaBrokersList;
	private static int parallelism;
	private static int parallelism2;
	//private static int multi;
	private static String kafkaOutputTopic;
	private static String kafkaUnionTopic;

	/**
	 * @param args Program arguments. You have to provide 4 arguments otherwise
	 *             DEFAULT values will be used.<br>
	 *             <ol>
	 *             <li>args[0]={@link #kafkaDataInputTopic} DEFAULT: "SpringI2")
	 *             <li>args[1]={@link #kafkaRequestInputTopic} DEFAULT: "rq13")
	 *             <li>args[2]={@link #kafkaBrokersList} (DEFAULT: "192.168.1.3:9092")
	 *             <li>args[3]={@link #parallelism} Job parallelism (DEFAULT: "6")
	 *             <li>
	 *             "O10")
	 *             </ol>
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {

		/*
		// Initialize Input Parameters
		initializeParameters(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		kafkaConsumer kc = new kafkaConsumer(kafkaBrokersList, kafkaDataInputTopic);
		kafkaConsumer requests = new kafkaConsumer(kafkaBrokersList, kafkaRequestInputTopic);
		//kafkaProducerEstimation kp = new kafkaProducerEstimation(kafkaBrokersList, kafkaOutputTopic);
		//kafkaProducerEstimation unionp = new kafkaProducerEstimation(kafkaBrokersList, kafkaUnionTopic);
		//kafkaProducerEstimation test = new kafkaProducerEstimation(kafkaBrokersList, "testPairs");

		DataStream<ObjectNode> RQ_stream = env.addSource(requests.getFc());
		DataStream<ObjectNode> datastream = env.addSource(kc.getFc()).setParallelism(parallelism2);


		//map kafka data input to tuple2<int,double>
		DataStream<Tuple2<String, String>> dataStream = datastream
				.map(new MapFunction<ObjectNode, Tuple2<String, String>>() {

					private static final long serialVersionUID = 1L;
				
					@Override
					public Tuple2<String, String> map(ObjectNode node) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<>(node.get("key").toString().replace("\"", ""), node.get("value").toString().replace("\"", ""));
				}
			}).setParallelism(parallelism2).keyBy((KeySelector<Tuple2<String, String>, String>) r -> r.f0);
		
		//DataStream<Tuple2<String, String>> dataStream = datastream.flatMap(new IngestionMultiplierFlatMap(multi)).setParallelism(parallelism2).keyBy(0);
		
		DataStream<Request> RQ_Stream = RQ_stream
				.map(new MapFunction<ObjectNode, Request>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Request map(ObjectNode node) throws Exception {
						// TODO Auto-generated method stub
						String[] valueTokens = node.get("value").toString().replace("\"", "").split(",");
						if(valueTokens.length > 5) {
						return new Request(node.get("key").toString().replace("\"", ""),valueTokens);
						}
						return null;
					}
				}).keyBy((KeySelector<Request, String>) r -> r.getKey());
			
		DataStream<Request> SynopsisRequests = RQ_Stream
				.flatMap(new RqRouterFlatMap()).keyBy((KeySelector<Request, String>) r -> r.getKey());
		
		DataStream<Tuple2<String, String>> DataStream = dataStream.connect(RQ_Stream)
				.flatMap(new dataRouterCoFlatMap()).keyBy((KeySelector<Tuple2<String, String>, String>) r -> r.f0);
		
		DataStream<Estimation> estimationStream = DataStream.connect(SynopsisRequests)
				.flatMap(new SDEcoFlatMap()).keyBy((KeySelector<Estimation, String>) r -> r.getKey());
       	
		//estimationStream.addSink(kp.getProducer());	
		//estimationStream.writeAsText("cm", FileSystem.WriteMode.OVERWRITE);
       
		SplitStream<Estimation> split = estimationStream.split(new OutputSelector<Estimation>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> select(Estimation value) {
				// TODO Auto-generated method stub
				 List<String> output = new ArrayList<String>();
				 if (value.getNoOfP() == 1) {
			            output.add("single");
			        }
			        else {
			            output.add("multy");
			        }
			        return output;
				}
			});   
		
		DataStream<Estimation> single = split.select("single");
		DataStream<Estimation> multy = split.select("multy");
		//single.addSink(kp.getProducer());

		//DataStream<Tuple2< String, Object>> finalStream = estimationStream.flatMap(new ReduceFlatMap());
		//multy.addSink(unionp.getProducer());
		
		@SuppressWarnings("unused")
		JobExecutionResult result = env.execute("Streaming miniSDE");

		 */

}

	private static void initializeParameters(String[] args) {

		if (args.length > 5) {
			
			System.out.println("[INFO] User Defined program arguments");
			//User defined program arguments
			kafkaDataInputTopic = args[0];
			kafkaRequestInputTopic = args[1];
			kafkaOutputTopic = args[2];
			kafkaUnionTopic = args[3];
			kafkaBrokersList = args[4];
			//kafkaBrokersList = "localhost:9092";
			parallelism = Integer.parseInt(args[5]);
			parallelism2 = Integer.parseInt(args[6]);
			//multi = Integer.parseInt(args[5]);

		}else{
			
			System.out.println("[INFO] Default values");
			//Default values
			kafkaDataInputTopic = "testSource3";
			kafkaRequestInputTopic = "testRequest6";
			kafkaUnionTopic = "testUnionTopic2";
			kafkaOutputTopic = "4FINOUT";
			parallelism = 3;
			parallelism2 = 3;
			//kafkaBrokersList = "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667";
			kafkaBrokersList = "localhost:9092";
			
			//multi =0;
			
		}
	}
}

