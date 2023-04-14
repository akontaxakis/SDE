package infore.SDE.experiments;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Datapoint;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.sources.kafkaProducerEstimation;
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

public class RUNRadiusTest {


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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        kafkaStringConsumer_Earliest kc = new kafkaStringConsumer_Earliest(kafkaBrokersList, kafkaDataInputTopic);
        kafkaStringConsumer_Earliest requests = new kafkaStringConsumer_Earliest(kafkaBrokersList, kafkaRequestInputTopic);
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
                }).name("DATA_SOURCE").keyBy((KeySelector<Datapoint, String>) Datapoint::getKey);

        //
        DataStream<Request> RQ_Stream = RQ_stream
                .map(new MapFunction<String, Request>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Request map(String node) throws IOException {
                        // TODO Auto-generated method stub
                        ObjectMapper objectMapper = new ObjectMapper();
                        Request request = objectMapper.readValue(node, Request.class);
                        return  request;
                    }
                }).name("REQUEST_SOURCE").setParallelism(1).keyBy((KeySelector<Request, String>) Request::getKey);

        DataStream<Request> SynopsisRequests = RQ_Stream
                .flatMap(new RqRouterFlatMap()).setParallelism(1).name("REQUEST_ROUTER");


        DataStream<Datapoint> DataStream = dataStream.connect(RQ_Stream)
                .flatMap(new dataRouterCoFlatMap()).setParallelism(1).name("DATA_ROUTER")
                .keyBy((KeySelector<Datapoint, String>) Datapoint::getKey);

        //Multiplication IF NEEDED
        //DataStream<Datapoint> DataStream2 = DataStream.flatMap(new IngestionMultiplierFlatMap(multi));

        DataStream<Estimation> estimationStream = DataStream.keyBy((KeySelector<Datapoint, String>) Datapoint::getKey)
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
        DataStream<Estimation> finalStream = multy.flatMap(new ReduceFlatMap()).name("REDUCE");



        //finalStream.addSink(kp.getProducer());
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
            Source ="non";
            kafkaBrokersList = "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667";
            kafkaOutputTopic = "RAD_OUT";

        }else{

            System.out.println("[INFO] Default values");
            kafkaDataInputTopic = "RAD_RR_mod5";
            kafkaRequestInputTopic = "RAD_REQUEST_6";
            Source ="non";
            multi = 10;
            parallelism = 4;
            //parallelism2 = 4;
            kafkaBrokersList = "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667";
            kafkaOutputTopic = "RAD_OUT";
        }
    }
}
