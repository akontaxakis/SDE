
import messages.Datapoint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.*;

public class SendFINData{

    public static void main(String[] args) throws Exception {
        String folderPath = "D:\\INFORE-data\\quotes2";
        //String requestTopic ="rq13";
        //String topicName = "sdeData";
        String topicName = "LEVEL1_1";
        String line = "";
        int jk, i, j;
        int nOfMessages = 0;
        int Gcount=0;

        final File folder = new File(folderPath);
        ArrayList<Tuple2<String, BufferedReader>> br = new ArrayList<Tuple2<String, BufferedReader>>();

        HashMap<String, Integer> mp =  new HashMap<String, Integer>();
        Properties props = new Properties();
        //props.put("bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");
        //props.put("bootstrap.servers","localhost:9092");
        props.put("bootstrap.servers","45.10.26.123:19092,45.10.26.123:29092,45.10.26.123:39092");

        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 163840);
        props.put("linger.ms", 0);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        //BufferedReader  br = new BufferedReader(new FileReader("C:\\Users\\ado.kontax\\Desktop\\data\\InforeWP2\\DataAPI_Source_code_Java\\DataAPI_Source_code_Java\\kafkaspringapi\\src\\springI1"));
        //for(int h = 0;h<1000; h++) {
            for (File fileEntry : folder.listFiles()) {
                if (fileEntry.isDirectory()) {

                    System.out.println(fileEntry.getAbsolutePath());

                    for (File fileEntry2 : fileEntry.listFiles()) {

                        BufferedReader br1 = new BufferedReader(new FileReader(fileEntry2.getAbsolutePath()));
                        String k = fileEntry2.getName().toString().replace(".txt", "");

                        boolean add;
                        String stock = k;
                        stock = stock.replace("╖", "");
                        stock = stock.replace("·", "");

                         add = br.add(new Tuple2<String, BufferedReader>(stock, br1));


                        j = 0;
                        int c = 0;
                        int size = br.size();
                        while (c < size) {
                            Iterator<Tuple2<String, BufferedReader>> e = br.iterator();

                            while (e.hasNext()) {

                                //System.out.println("oti nane");
                                //br1.readLine().replace(".","").split(",")
                                Tuple2<String, BufferedReader> br2 = e.next();
                                // System.out.println("key-> " + br2.getKey());
                                BufferedReader x =  br2.getField(1);

                                line =x.readLine();
                                String second = br2.getField(0);
                                String stock2 = second.replace(" ", "");
                                stock2 = stock2.replace("╖", "");
                                stock2 = stock2.replace("·", "");


                                if (line == null) {
                                    e.remove();
                                    c++;
                                } else {

                                    e.remove();
                                    c++;

                                    String[] words = new String[4];
                                    StringTokenizer tokenizer = new StringTokenizer(line, ",");
                                    //System.out.println(line);
                                    for (jk = 0; jk < 4; jk++) {
                                        words[jk] = tokenizer.nextToken();
                                    }

                                    if( stock2.startsWith("Forex")) {

                                        String jsonString = "{\"DateTime\":\"" + words[0] + " " + words[1] + "\",\"StockID\":\"" + stock2 + "\",\"price\":\"" + words[2] +"\",\"Volume\":\"" + words[3] + "\"}";
                                        System.out.println(jsonString);
                                        //ObjectMapper mapper = new ObjectMapper();
                                        //JsonNode node = mapper.readTree(jsonString);
                                        //String phoneType = node.get("phonetype").asText();
                                        //String cat = node.get("cat").asText();

                                        Datapoint dp = new Datapoint("Forex", stock2, jsonString);
                                        //Datapoint dp =  new Datapoint("Forex", stock2,words[0]+" "+words[1]+","+words[2]);
                                        producer.send(new ProducerRecord<String, String>(topicName, jsonString));
                                        //System.out.println(dp.toJsonString());
                                        ObjectMapper objectMapper = new ObjectMapper();
                                       // Datapoint dp2 = objectMapper.readValue(dp.toJsonString(), Datapoint.class);
                                        //System.out.println(dp2.toJsonString());

                                        ObjectMapper mapper = new ObjectMapper();
                                        //JsonNode node = mapper.readTree(dp2.getValues());
                                        //String phoneType = node.get("price").asText();
                                        //String cat = node.get("StockID").asText();
                                        nOfMessages++;
                                        System.out.println(nOfMessages + " id ->" + stock2);
                                    }
                                }

                            }
                        }
                    }
                }
            }
       // }
        System.out.println(line);
        System.out.println("Message sent successfully -> " + nOfMessages);
        producer.close();
    }
}