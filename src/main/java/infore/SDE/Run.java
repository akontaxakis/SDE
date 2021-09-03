package infore.SDE;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Datapoint;
import infore.SDE.sources.kafkaProducerEstimation;
import infore.SDE.sources.kafkaStringConsumer;

import infore.SDE.transformations.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;


/**
 * <br>
 * Implementation code for SDE for INFORE-PROJECT" <br> *
 * ATHENA Research and Innovation Center <br> *
 * Author: Antonis_Kontaxakis <br> *
 * email: adokontax15@gmail.com *
 */

@SuppressWarnings("deprecation")
public class Run {

	private static String kafkaDataInputTopic;
	private static String kafkaRequestInputTopic;
	private static String kafkaBrokersList;
	private static int parallelism;
	private static String kafkaOutputTopic;

	/**
	 * @param args Program arguments. You have to provide 4 arguments otherwise
	 *             DEFAULT values will be used.<br>
	 *             <ol>
	 *             <li>args[0]={@link #kafkaDataInputTopic} DEFAULT: "Forex")
	 *             <li>args[1]={@link #kafkaRequestInputTopic} DEFAULT: "Requests")
	 *             <li>args[2]={@link #kafkaBrokersList} (DEFAULT: "localhost:9092")
	 *             <li>args[3]={@link #parallelism} Job parallelism (DEFAULT: "4")
	 *             <li>args[4]={@link #kafkaOutputTopic} DEFAULT: "OUT")
	 *             "O10")
	 *             </ol>
	 *
	 */

	public static void main(String[] args) throws Exception {
		// Initialize Input Parameters
		initializeParameters(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		kafkaStringConsumer kc = new kafkaStringConsumer(kafkaBrokersList, kafkaDataInputTopic);
		kafkaStringConsumer requests = new kafkaStringConsumer(kafkaBrokersList, kafkaRequestInputTopic);
		kafkaProducerEstimation kp = new kafkaProducerEstimation(kafkaBrokersList, kafkaOutputTopic);
		kafkaProducerEstimation pRequest = new kafkaProducerEstimation(kafkaBrokersList, kafkaRequestInputTopic);

		DataStream<String> datastream = env.addSource(kc.getFc());
		DataStream<String> RQ_stream = env.addSource(requests.getFc());

		//map kafka data input to tuple2<int,double>
		DataStream<Datapoint> dataStream = datastream
				.map(new MapFunction<String, Datapoint>() {
					@Override
					public Datapoint map(String node) throws IOException {
						// TODO Auto-generated method stub
						ObjectMapper objectMapper = new ObjectMapper();
						Datapoint dp = objectMapper.readValue(node, Datapoint.class);
						return dp;
					}
			}).name("DATA_SOURCE").keyBy((KeySelector<Datapoint, String>)Datapoint::getKey);
		
		//DataStream<Tuple2<String, String>> dataStream = datastream.flatMap(new IngestionMultiplierFlatMap(multi)).setParallelism(parallelism2).keyBy(0);
		DataStream<Request> RQ_Stream = RQ_stream
				.map(new MapFunction<String, Request>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Request map(String node) throws IOException {
						// TODO Auto-generated method stub
						//String[] valueTokens = node.replace("\"", "").split(",");
						//if(valueTokens.length > 6) {
						ObjectMapper objectMapper = new ObjectMapper();

						// byte[] jsonData = json.toString().getBytes();
						Request request = objectMapper.readValue(node, Request.class);
						return  request;
					}
				}).name("REQUEST_SOURCE").keyBy((KeySelector<Request, String>) Request::getKey);

		DataStream<Request> SynopsisRequests = RQ_Stream
				.flatMap(new RqRouterFlatMap()).name("REQUEST_ROUTER");


		DataStream<Datapoint> DataStream = dataStream.connect(RQ_Stream)
				                                .flatMap(new dataRouterCoFlatMap()).name("DATA_ROUTER")
												.keyBy((KeySelector<Datapoint, String>) Datapoint::getKey);


		DataStream<Estimation> estimationStream = DataStream.keyBy((KeySelector<Datapoint, String>) Datapoint::getKey)
				.connect(SynopsisRequests.keyBy((KeySelector<Request, String>) Request::getKey))
				.flatMap(new SDEcoFlatMap()).name("SYNOPSES_MAINTENANCE");




		SplitStream<Estimation> split = estimationStream.split(new OutputSelector<Estimation>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> select(Estimation value) {
				// TODO Auto-generated method stub
				 List<String> output = new ArrayList<>();
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
		DataStream<Estimation> multy = split.select("multy").keyBy((KeySelector<Estimation, String>) Estimation::getKey);
		single.addSink(kp.getProducer());
		DataStream<Estimation> partialOutputStream = multy.flatMap(new ReduceFlatMap()).name("REDUCE");

		DataStream<Estimation> finalStream = partialOutputStream.flatMap(new GReduceFlatMap()).setParallelism(1);


		SplitStream<Estimation> split_2 = finalStream.split(new OutputSelector<Estimation>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> select(Estimation value) {
				// TODO Auto-generated method stub
				List<String> output = new ArrayList<>();
				if (value.getRequestID() == 7) {
					output.add("UR");
				}
				else {
					output.add("E");
				}
				return output;
			}
		});

		DataStream<Estimation> UR = split_2.select("UR");
		DataStream<Estimation> E = split_2.select("E");
		//E.addSink(kp.getProducer());
		//UR.addSink(pRequest.getProducer());

		finalStream.addSink(kp.getProducer());
		env.execute("Streaming SDE");

}

	private static void initializeParameters(String[] args) {

		if (args.length > 4) {

			System.out.println("[INFO] User Defined program arguments");
			//User defined program arguments
			kafkaDataInputTopic = args[0];
			kafkaRequestInputTopic = args[1];
			kafkaOutputTopic = args[2];
			kafkaBrokersList = args[3];
			//kafkaBrokersList = "localhost:9092";
			parallelism = Integer.parseInt(args[4]);
			//parallelism2 = Integer.parseInt(args[5]);
			//multi = Integer.parseInt(args[5]);

		}else{
			
			System.out.println("[INFO] Default values");
			//Default values
			//kafkaDataInputTopic = "FAN";
			kafkaDataInputTopic = "FIN_DATA";
			kafkaRequestInputTopic = "FIN_RQ";
			//kafkaRequestInputTopic = "Rq_FAN";
			parallelism = 4;
			//parallelism2 = 4;
			//kafkaBrokersList = "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667";
			kafkaBrokersList = "45.10.26.123:19092";
			//kafkaBrokersList = "159.69.32.166:9092";
			kafkaOutputTopic = "FIN_OUT";
		}
	}
}
