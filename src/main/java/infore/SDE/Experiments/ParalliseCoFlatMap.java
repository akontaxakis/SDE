package infore.SDE.Experiments;

import infore.SDE.messages.Datapoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Random;

public class ParalliseCoFlatMap implements FlatMapFunction<Datapoint, Datapoint> {

    /**
     *
     */

    private static final long serialVersionUID = 1L;

    private int count = 0;
    private int threshold =70;

    @Override
    public void flatMap(Datapoint value, Collector<Datapoint> out) throws Exception {
        count++;
        Random R = new Random();
        if(count>100) {
            for(int i=0;i<10;i++){
                value.setDataSetkey("D"+i);
                out.collect(value);
                count =0;
            }
        }else{
            value.setDataSetkey("D"+R.nextInt(10));
            out.collect(value);
        }
    }


}


