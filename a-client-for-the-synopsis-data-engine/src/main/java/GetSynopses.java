import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.queryablestate.client.state.ImmutableValueState;
import org.junit.Assert;
import java.util.concurrent.CompletableFuture;

public class GetSynopses {

    public static void main(String[] args) throws Exception {

        //Flink Cluster from which you want to get the Synopses
        int proxyPort = 9069;
        String tmHostname = "localhost";
        QueryableStateClient client = new QueryableStateClient(tmHostname, proxyPort);

        //DataSetKey for which you want to get its Synopses
        //String key = "FINANCIAL_USECASE";


        String key = "FOREX";
        //the jobID
        JobID jid = new JobID();
        jid = jid.fromHexString("b641814ef72e2405cfd0317f20e14fde");
        // the state descriptor of the state to be fetched.
        ValueStateDescriptor<Tuple1<String>> descriptor =
                new ValueStateDescriptor<Tuple1<String>>(
                        "Synopses",
                        TypeInformation.of(new TypeHint<Tuple1<String>>() {
                        }).createSerializer(new ExecutionConfig()),new Tuple1<String>("JK"));



        TypeHint<String> typeHint = new TypeHint<String>() {};

        CompletableFuture<ValueState<Tuple1<String>>> resultFuture = client.getKvState(jid, "getSynopses", key, BasicTypeInfo.STRING_TYPE_INFO, descriptor);


        //CompletableFuture<ValueState<Tuple1<String>>> resultFuture = client.getKvState(jid, "getSynopses", key, typeHint, descriptor);
       /* CompletableFuture<ValueState<String>> getKvState =
                client.getKvState(JobID.fromHexString(”JobId”),”request”,
        key,TypeInformation.of(newTypeHint<String>(){ }),
        mydescriptor);*/


        // now handle the returned value
       //resultFuture.
        ImmutableValueState<Tuple1<String>> res2 = (ImmutableValueState<Tuple1<String>>)resultFuture.get();
       System.out.println(res2.value().f0);
        /*
            resultFuture.thenAccept(response -> {
                try {

                    ValueState<Tuple1<String>> res = resultFuture.get();
                    System.out.println(res.value().f0);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            });
        } */
    }
    }