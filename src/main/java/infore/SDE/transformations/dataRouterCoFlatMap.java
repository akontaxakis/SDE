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

import java.util.*;

public class dataRouterCoFlatMap extends RichCoFlatMapFunction<Datapoint, Request, Datapoint> {

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
    @Override
    public void flatMap1(Datapoint value, Collector<Datapoint> out) throws Exception {



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
            if(tmp.size()==0){
                System.out.println(" "+value.getStreamID()+" "+tmp.size());
            }
            if(tmp.size()>1){
                System.out.println("kati");
            }
            for (Tuple2<Integer, String> t : tmp) {
                value.setDataSetkey(t.f1);
                out.collect(value);
            }

        }
        if(RandomParallelism.size()>0){
            System.out.println("kati_kati");
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

        if(rq.getRequestID()  == 5)
            rq.setRequestID(1);

        if (rq.getRequestID()  == 1 || rq.getRequestID()  == 4 ) {

            if (rq.getNoOfP() > 1) {

                if (rq.getRequestID()  == 1){
                    if(KeyedParallelism.get(rq.getNoOfP())==null) {
                        KeyedParallelism.put(rq.getNoOfP(), new Tuple2<>(1, 0));
                        int i = 0;
                        for(ArrayList<Tuple2<Integer,String>> v : KeysPerStream.values()) {
                             v.add(new Tuple2<>(rq.getNoOfP(), rq.getKey()+"_"+rq.getNoOfP()+"_KEYED_" + i));
                             i++;
                             if(i==rq.getNoOfP())
                                 i=0;
                        }
                    }
                    else{
                        Tuple2<Integer, Integer> t =  KeyedParallelism.get(rq.getNoOfP());
                        t.f0 += 1;
                        System.out.println("HERE_> " + t.f0);
                        KeyedParallelism.put(rq.getNoOfP(),t);
                    }
                }else if (rq.getRequestID()  == 4){
                    if(RandomParallelism.get(rq.getNoOfP())==null) {
                        KeyedParallelism.put(rq.getNoOfP(), new Tuple2<>(1, 0));
                    }
                    else{
                        Tuple2<Integer, Integer> t =  RandomParallelism.get(rq.getNoOfP());
                        t.f0++;
                        RandomParallelism.put(rq.getNoOfP(),t);
                    }


                }

            }
            String newS = rq.toSumString();

            if (rs.value() == null) {

                Tuple1<String> tmp = new Tuple1<String>(newS);
                Synopses.put(""+rq.getUID(), rq);
                rs.update(tmp);

                //System.out.print("\n StateInit");
            } else {

                Synopses.put("" +rq.getUID(), rq);
                Tuple1<String> tmp = rs.value();
                tmp.f0 = newS.concat(tmp.f0);
                //System.out.print("\n StateUpdate");
                rs.update(tmp);
            }
        }
        else if (rq.getRequestID() == 2) {

            Request re = Synopses.remove("" + rq.getUID());
            if (re != null) {

                if(re.getRequestID() == 1){
                    Tuple2<Integer, Integer> t = KeyedParallelism.get(re.getNoOfP());
                    if(t!=null) {
                        System.out.println("HERE_> " + t.f0);
                        t.f0--;
                        if (t.f0 == 0) {
                            KeyedParallelism.remove(re.getNoOfP());
                            for (ArrayList<Tuple2<Integer, String>> v : KeysPerStream.values()) {
                                for (Tuple2<Integer, String> t2 : v) {
                                    if (t2.f0 == re.getNoOfP()) {
                                        v.remove(t2);
                                        break;
                                    }
                                }
                            }

                        } else {
                            KeyedParallelism.put(re.getNoOfP(), t);
                        }
                    }
                }else if(re.getRequestID() == 4) {
                    Tuple2<Integer, Integer> t = RandomParallelism.get(re.getNoOfP());
                    t.f0--;
                    if (t.f0 == 0) {
                        RandomParallelism.remove(re.getNoOfP());
                    }
                }

                String tmp = "";
                if (Synopses.size() > 0) {
                    for (Request entry : Synopses.values()) {
                        tmp = entry.toSumString().concat(tmp);
                    }

                    Tuple1<String> tmp2 = new Tuple1<>(tmp);
                    rs.update(tmp2);

                } else {
                    rs.update(new Tuple1<String>(""));
                }
            }
        }
    }

    public void open(Configuration config) {


        TypeInformation<Tuple1<String>> typeInformation = TypeInformation.of(new TypeHint<Tuple1<String>>() {});

        ValueStateDescriptor descriptor = new ValueStateDescriptor("Synopses", typeInformation);
        descriptor.setQueryable("getSynopses");
        rs = getRuntimeContext().getState(descriptor);
        pId = getRuntimeContext().getIndexOfThisSubtask();

    }

}


