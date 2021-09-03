package infore.SDE.synopses;

import lib.TopK.TopK;
import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import lib.WLSH.Bucket;
import lib.WLSH.WLSHsynopsis;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;

public class SynopsisTopK extends Synopsis{

        private  TopK Synopsis;
        String[] parameters;
        private String timeIndex;

        public SynopsisTopK(int uid, String[] param) {
            super(uid, param[0], param[1]);
            timeIndex = param[2];
            Synopsis = new TopK(Integer.parseInt(param[3]),Integer.parseInt(param[4]));
            parameters = param;
        }
        @Override
        public void add(Object k) {

            JsonNode node = (JsonNode)k;
            String key = node.get(this.keyIndex).asText();
            String value = node.get(this.valueIndex).asText();
            int time = Integer.parseInt(node.get(this.timeIndex).asText());
            Synopsis.add(key,value,time);

        }
        @Override
        public Object estimate(Object k) {
            // TODO Auto-generated method stub
            return null;
        }
        @Override
        public Synopsis merge(Synopsis sk) {
            // TODO Auto-generated method stub
            return null;
        }
        public Estimation estimate(Request rq) {
            return new Estimation(rq, Synopsis.estimate(), Integer.toString(rq.getUID()));
        }



}