package infore.SDE.synopses;

import TopK.TopK;
import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;

public class SynopsisTopK extends Synopsis{

        private HashMap<String, TopK> Synopses;
        String[] parameters;

        public SynopsisTopK(int uid, String[] param) {
            super(uid, param[0], param[1], param[2]);
            Synopses = new HashMap<>();
            parameters = param;
        }
        @Override
        public void add(Object k) {

            JsonNode node = (JsonNode)k;
            DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss", Locale.ENGLISH);
            String key = node.get(this.keyIndex).asText();
            String value = node.get(this.valueIndex).asText();
            TopK tpK = Synopses.get(key);
            if(tpK == null)
                tpK = new TopK(Integer.parseInt(parameters[3]),Integer.parseInt(parameters[4]));

            tpK.add(key,value);
            Synopses.put(key, tpK);

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
            return null;
        }

}