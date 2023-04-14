package infore.SDE.Experiments;
import infore.SDE.producersForTesting.sendSpringTest;


public class RunSpringTest {

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

            Thread thread1 = new Thread(() -> {
                (new sendSpringTest()).run(kafkaBrokersList, kafkaDataInputTopic, kafkaRequestInputTopic);
            });
            thread1.start();


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
            kafkaDataInputTopic = "FIN_DATA";
            kafkaRequestInputTopic = "FIN_RQ";
            parallelism = 4;
            kafkaBrokersList = "45.10.26.123:19092";
            kafkaOutputTopic = "FIN_OUT";
        }
    }
}
