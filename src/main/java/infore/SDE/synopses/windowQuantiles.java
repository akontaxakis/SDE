package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import org.streaminer.stream.quantile.WindowSketchQuantiles;

public class windowQuantiles extends Synopsis{

    private WindowSketchQuantiles wq;

    public windowQuantiles(int ID, String[] parameters) {
        super(ID, parameters[0],parameters[1],parameters[2]);
        wq = new WindowSketchQuantiles(Double.parseDouble(parameters[3]));
        wq.setWindowSize(Integer.parseInt(parameters[4]));
    }

    @Override
    public void add(Object k) {
        JsonNode node = (JsonNode)k;

        String value = node.get(this.valueIndex).asText();
        wq.offer(Double.parseDouble(value));
    }

    @Override
    public Object estimate(Object k) {
        return null;
    }

    @Override
    public Estimation estimate(Request rq) {
        try {
            return new Estimation(rq, wq.getQuantile(Double.parseDouble(rq.getParam()[0])), Integer.toString(rq.getUID()));
        } catch (Exception e) {
            return new Estimation(rq, null, Integer.toString(rq.getUID()));
        }
    }
    @Override
    public Synopsis merge(Synopsis sk) {
        return null;
    }
}
