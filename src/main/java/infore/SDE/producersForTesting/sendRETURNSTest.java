package infore.SDE.producersForTesting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Request;
import javafx.util.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static infore.SDE.producersForTesting.Test_Return_FIN_Data.*;

public class sendRETURNSTest {

    public void run(String kafkaDataInputTopic, String topicRequests) {
        try {
            SendaddRequest(topicRequests,"cluster");
            sendFINPrices(kafkaDataInputTopic);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}