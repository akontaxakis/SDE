package infore.SDE;



import infore.SDE.producersForTesting.sendSIMTest;
import infore.SDE.producersForTesting.sendTopKTest;



/**
 * <br>
 * Implementation code for SDE for INFORE-PROJECT" <br> *
 * ATHENA Research and Innovation Center <br> *
 * Author: Antonis_Kontaxakis <br> *
 * email: adokontax15@gmail.com *
 */

@SuppressWarnings("deprecation")
public class DataTests {

    private static String kafkaDataInputTopic;
    private static String kafkaRequestInputTopic;
    private static String kafkaBrokersList;
    private static int parallelism;
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

      /*  if(Source.startsWith("auto")) {
            Thread thread1 = new Thread(() -> {
                (new sendFINTest()).run(kafkaDataInputTopic);
            });
            thread1.start();
        }*/
        if(Source.startsWith("auto")) {
            Thread thread1 = new Thread(() -> {
                (new sendSIMTest()).run(kafkaDataInputTopic);
            });
            thread1.start();
        }



    }

    private static void initializeParameters(String[] args) {

        if (args.length > 4) {

            System.out.println("[INFO] User Defined program arguments");
            //User defined program arguments
            kafkaDataInputTopic = args[0];
            kafkaRequestInputTopic = args[1];
            kafkaOutputTopic = args[2];
            kafkaBrokersList = args[3];
            Source ="auto";
            //kafkaBrokersList = "localhost:9092";
            parallelism = Integer.parseInt(args[4]);
            //parallelism2 = Integer.parseInt(args[5]);
            //multi = Integer.parseInt(args[5]);

        }else{

            System.out.println("[INFO] Default values");
            //Default values
            //kafkaDataInputTopic = "FAN";
            kafkaDataInputTopic = "BIO_DATA";
            kafkaRequestInputTopic = "BIO_Rq_3";
            Source ="auto";
            //kafkaRequestInputTopic = "Rq_FAN";
            parallelism = 4;
            //parallelism2 = 4;
            //kafkaBrokersList = "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667";
            kafkaBrokersList = "localhost:9092";
            //kafkaBrokersList = "159.69.32.166:9092";
            kafkaOutputTopic = "OUT_M";
        }
    }
}
