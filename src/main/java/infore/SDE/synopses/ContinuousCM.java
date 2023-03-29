package infore.SDE.synopses;


import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.synopses.Sketches.CM;


public class ContinuousCM extends ContinuousSynopsis{
    private CM cm;


    public ContinuousCM(int uid, Request t_rq, String[] parameters) {
        super(uid,parameters[0],parameters[1], parameters[2]);
        cm = new CM(Double.parseDouble(parameters[3]),Double.parseDouble(parameters[3]),Integer.parseInt(parameters[4]));
        this.setRq(t_rq);
    }

    @Override
    public void add(Object k) {
        //String j = (String)k;

        //ObjectMapper mapper = new ObjectMapper();
        JsonNode node = (JsonNode)k;
        /*try {
            node = mapper.readTree(j);
        } catch (IOException e) {
            e.printStackTrace();
        } */

        String key = node.get(this.keyIndex).asText();

        if(this.valueIndex.startsWith("null")){

            cm.add(key, 1);
        }else{
            String value = node.get(this.valueIndex).asText();
            //cm.add(Math.abs((key).hashCode()), (long)Double.parseDouble(value));
            cm.add(key, (long)Double.parseDouble(value));
        }


    }

    @SuppressWarnings("deprecation")
    @Override
    public Object estimate(Object k)
    {
        return Long.toString(cm.estimateCount((long) k));
    }

    @Override
    public Synopsis merge(Synopsis sk) {
        return sk;
    }

    @Override
    public Estimation estimate(Request rq) {
        //System.out.println(Math.abs(rq.getParam()[0].hashCode())+"_"+(double) cm.estimateCount(Math.abs(rq.getParam()[0].hashCode())));
        //return new Estimation(rq, Double.toString((double)cm.estimateCount(Math.abs(rq.getParam()[0].hashCode()))), Integer.toString(rq.getUID()));
        return new Estimation(rq, Double.toString((double)cm.estimateCount(Math.abs(rq.getParam()[0].hashCode()))), Integer.toString(rq.getUID()));
    }


    @Override
    public Estimation addEstimate(Object k) {
        add(k);
        //String j = (String)k;

        //ObjectMapper mapper = new ObjectMapper();
        JsonNode node = (JsonNode)k;
        /*try {
            node = mapper.readTree(j);
        } catch (IOException e) {
            e.printStackTrace();
        } */
        String key = node.get(this.keyIndex).asText();
        String e = Double.toString((double)cm.estimateCount(key));
        if(e != null) {
            Estimation es = new Estimation(rq, e, Integer.toString(rq.getUID()));
            es.setEstimationkey(key);
            return es;
        }else{
            return null;
        }
    }

}
