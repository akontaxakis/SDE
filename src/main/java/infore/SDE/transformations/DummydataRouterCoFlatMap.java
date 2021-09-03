package infore.SDE.transformations;

import infore.SDE.messages.Datapoint;
import infore.SDE.messages.Request;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DummydataRouterCoFlatMap extends RichCoFlatMapFunction<Datapoint, Request, Datapoint> {

    private static final long serialVersionUID = 1L;
    // SourceID (1-5), StreamID(1-10000), Keys(1-1000),

    //UID, parallelism, index,
    //Parallelism //Number, index
    private HashMap<Integer, Tuple2<Integer,Integer>> KeyedParallelism = new HashMap<>();
    //Parallelism //Number, index
    private HashMap<Integer, Tuple2<Integer,Integer>> RandomParallelism = new HashMap<>();
    //StreamID                 //Parallelism, KEYs
    private HashMap<String, ArrayList<Tuple2<Integer,String>>> KeysPerStream =  new HashMap<>();
    private HashMap<String,Request> Synopses =  new HashMap<>();
    private transient ValueState<Tuple1<String>> rs;
    private int pId;
    private int p;
    private int i;
    public DummydataRouterCoFlatMap(int p1){
        p=p1;
        i = 0;
    }
    @Override
    public void flatMap1(Datapoint value, Collector<Datapoint> out) throws Exception {


        if(i>=p)
            i=0;
        value.setDataSetkey(value.getDataSetkey() +"_"+p+"_KEYED_" + i);
        i++;
        out.collect(value);


        //Send Data with default Key the StreamID
        // value.f0 = value_tokens[0];
        //out.collect(value);
        //System.out.println(pId+" size "+ KeyedParallelism.size());
        if (KeyedParallelism.size() > 0) {
            ArrayList<Tuple2<Integer, String>> tmp = KeysPerStream.get(value.getStreamID());
            if (tmp == null) {
                tmp = new ArrayList<>();
                for (Map.Entry<Integer, Tuple2<Integer, Integer>> entry : KeyedParallelism.entrySet()) {
                    Integer key = entry.getKey();
                    Tuple2<Integer, Integer> v = entry.getValue();
                    tmp.add(new Tuple2<>(key, value.getDataSetkey() +"_"+key+"_KEYED_" + v.f1));
                    v.f1++;
                    if (v.f1 >= key) {
                        //v.f1 = 0;
                        // System.out.println(v.f1);
                        entry.setValue(new Tuple2<>(key,0));
                    }
                }
                KeysPerStream.put(value.getStreamID(), tmp);
            }
            for (Tuple2<Integer, String> t : tmp) {
                value.setDataSetkey(t.f1);
                //out.collect(value);
            }

        }
        if(RandomParallelism.size() > 0){
            for (Map.Entry<Integer, Tuple2<Integer, Integer>> entry : RandomParallelism.entrySet()) {
                Integer key = entry.getKey();
                Tuple2<Integer, Integer> v = entry.getValue();
                value.setDataSetkey(value.getDataSetkey() +"_"+key+"_RANDOM_" + v.f1);
                out.collect(value);
                v.f1++;
                if (v.f1 == key) {

                    v.f1 = 0;
                    entry.setValue(v);
                }
            }
        }
    }
    @Override
    public void flatMap2(Request rq, Collector<Datapoint> out) throws Exception {


    }

    public void open(Configuration config) {


        pId = getRuntimeContext().getIndexOfThisSubtask();

    }

}