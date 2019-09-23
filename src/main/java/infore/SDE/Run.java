package infore.SDE;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import infore.SDE.sources.kafkaConsumer;
import infore.SDE.sources.kafkaProducer;

/**
 * <br>
 * Implementation code of the SDE for INFORE-PROJECT" <br>
 * TECHNICAL UNIVERSITY OF CRETE <br>
 * Author:Antonis_Kontaxakis <br>
 * email: adokontax15@gmail.com
 */

public class Run {

	private static String kafkaDataInputTopic;
	private static String kafkaRequestInputTopic;
	private static String kafkaBrokersList;
	private static int parallelism;
	private static String kafkaOutputTopic;

	/**
	 * @param args Program arguments. You have to provide 8 arguments otherwise
	 *             DEFAULT values will be used.<br>
	 * 
	 *             <ol>
	 *             <li>args[0]={@link #kafkaDataInputTopic} DEFAULT: "SpringI2")
	 *             <li>args[1]={@link #kafkaRequestInputTopic} DEFAULT: "rq13")
	 *             <li>args[2]={@link #kafkaBrokersList} (DEFAULT: "localhost:9092")
	 *             <li>args[3]={@link #parallelism} Job parallelism (DEFAULT: "4")
	 *             <li>args[4]={@link #kafkaOutputTopic} Point dimension (DEFAULT:
	 *             "O10")
	 *             </ol>
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// Initialize Input Parameters
		initializeParameters(args);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		kafkaConsumer kc = new kafkaConsumer(kafkaBrokersList, kafkaDataInputTopic);
		kafkaConsumer requests = new kafkaConsumer(kafkaBrokersList, kafkaDataInputTopic);

		kafkaProducer kp = new kafkaProducer(kafkaBrokersList, kafkaOutputTopic);

		DataStream<ObjectNode> datastream = env.addSource(kc.getFc());
		DataStream<ObjectNode> RQ_stream = env.addSource(requests.getFc());

		// map kafka data input to tuple2<int,double>
		DataStream<Tuple2<Integer, Double>> dataStream = datastream
				.map(new MapFunction<ObjectNode, Tuple2<Integer, Double>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Double> map(ObjectNode node) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<>(node.get("key").asInt(), node.get("value").asDouble());
					}

				}).keyBy(0);

		// map kafka request input to tuple2<int, String> where String is a list of
		// parameters
		DataStream<Tuple2<Integer, String>> rqStream = RQ_stream
				.map(new MapFunction<ObjectNode, Tuple2<Integer, String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> map(ObjectNode node) throws Exception {
						return new Tuple2<>(node.get("key").asInt(), node.get("value").toString());
					}
				}).keyBy(0);

		DataStream<Tuple2<Integer, Object>> estimationStream = dataStream.connect(rqStream)
				.flatMap(new SDEcoFlatMap());
       
		estimationStream.addSink(kp.getProducer());
		// estimationStream.writeAsText("cm", FileSystem.WriteMode.OVERWRITE);

		@SuppressWarnings("unused")
		JobExecutionResult result = env.execute("Streaming SDE");

	}

	private static void initializeParameters(String[] args) {

		if (args.length == 5) {
			System.out.println("[INFO] User defined program arguments");
			// User defined program arguments
			kafkaDataInputTopic = args[0];
			kafkaRequestInputTopic = args[1];
			kafkaBrokersList = args[2];
			parallelism = Integer.parseInt(args[3]);
			kafkaOutputTopic = args[4];

		} else {
			System.out.println("[INFO] Default values");
			// Default values

			kafkaDataInputTopic = "SpringI2";
			kafkaRequestInputTopic = "rq13";
			kafkaBrokersList = "localhost:9092";
			parallelism = 4;
			kafkaOutputTopic = "O10";
		}

	}

}
