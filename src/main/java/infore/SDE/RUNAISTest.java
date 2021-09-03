package infore.SDE;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Datapoint;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.producersForTesting.sendAISTest;
import infore.SDE.producersForTesting.sendSIMTest;
import infore.SDE.sources.kafkaProducerEstimation;
import infore.SDE.sources.kafkaStringConsumer;
import infore.SDE.sources.kafkaStringConsumer_Earliest;
import infore.SDE.transformations.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RUNAISTest {


    private static String kafkaDataInputTopic;
    private static String kafkaRequestInputTopic;
    private static String kafkaBrokersList;
    private static int parallelism;
    private static int multi;
    private static String kafkaOutputTopic;
    private static String Source;

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

        if(Source.startsWith("auto")) {
            Thread thread1 = new Thread(() -> {
                (new sendAISTest()).run(kafkaDataInputTopic,kafkaRequestInputTopic,parallelism);
            });
            thread1.start();
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        kafkaStringConsumer_Earliest kc = new kafkaStringConsumer_Earliest(kafkaBrokersList, kafkaDataInputTopic);
        kafkaStringConsumer requests = new kafkaStringConsumer(kafkaBrokersList, kafkaRequestInputTopic);
        kafkaProducerEstimation kp = new kafkaProducerEstimation(kafkaBrokersList, kafkaOutputTopic);
        kafkaProducerEstimation pRequest = new kafkaProducerEstimation(kafkaBrokersList, kafkaRequestInputTopic);
        //kafkaProducerEstimation test = new kafkaProducerEstimation(kafkaBrokersList, "testPairs");

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
                }).name("DATA_SOURCE");

        //
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
                .flatMap(new DummydataRouterCoFlatMap(parallelism)).name("DATA_ROUTER")
                .keyBy((KeySelector<Datapoint, String>) Datapoint::getKey);

        DataStream<Datapoint> DataStream2 = DataStream.flatMap(new IngestionMultiplierFlatMap(multi));

        DataStream<Estimation> estimationStream = DataStream2.keyBy((KeySelector<Datapoint, String>) Datapoint::getKey)
                .connect(SynopsisRequests.keyBy((KeySelector<Request, String>) Request::getKey))
                .flatMap(new SDEcoFlatMap()).name("SYNOPSES_MAINTENANCE").disableChaining();




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
        //single.addSink(kp.getProducer());
        DataStream<Estimation> partialOutputStream = multy.flatMap(new ReduceFlatMap()).name("REDUCE").setParallelism(1);

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

        finalStream.addSink(kp.getProducer()).setParallelism(1);
        env.execute("Streaming SDE"+parallelism+"_"+multi+"_"+kafkaDataInputTopic);

    }

    private static void initializeParameters(String[] args) {

        if (args.length > 1) {

            System.out.println("[INFO] User Defined program arguments");
            //User defined program arguments
            kafkaDataInputTopic = args[0];
            kafkaRequestInputTopic = args[1];
            multi = Integer.parseInt(args[2]);
            parallelism = Integer.parseInt(args[3]);
            System.out.println("[INFO] Default values");
            //Default values
            //kafkaDataInputTopic = "FAN";
            Source ="auto";
            //kafkaRequestInputTopic = "Rq_FAN";

            //parallelism2 = 4;
            kafkaBrokersList = "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667";
            //kafkaBrokersList = "localhost:9092";
            //kafkaBrokersList = "159.69.32.166:9092";
            kafkaOutputTopic = "AIS_OUT";

        }else{

            System.out.println("[INFO] Default values");
            //Default values
            //kafkaDataInputTopic = "FAN";
            kafkaDataInputTopic = "AIS_DATA_10000";
            kafkaRequestInputTopic = "Rq_AIS";
            Source ="auto";
            multi = 10;
            //kafkaRequestInputTopic = "Rq_FAN";
            parallelism = 2;
            //parallelism2 = 4;
            kafkaBrokersList = "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667";
            //kafkaBrokersList = "localhost:9092";
            //kafkaBrokersList = "159.69.32.166:9092";
            kafkaOutputTopic = "AIS_OUT";
        }
    }
}
