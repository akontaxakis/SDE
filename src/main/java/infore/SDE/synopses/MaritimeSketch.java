package infore.SDE.synopses;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicMask;

import java.io.IOException;
import java.util.LinkedList;

public class MaritimeSketch extends Synopsis {
	
    private static final ObjectMapper jackson_mapper = new ObjectMapper();
    private int msp;
    private double md, min_knots_diff, min_course_diff;
    private static final LinkedList<String> positions = new LinkedList<String>();

    public MaritimeSketch(int ID, String k, String v, int minsamplingperiod, double minimumDistance, double knots,double corse ) {
        super(ID, k, v);
        msp = minsamplingperiod;
        md = minimumDistance;
        min_knots_diff = knots;
    }

    public void add(Object n) {
    	String k = (String)n;
        ObjectNode curr = null;
		try {
			curr = (ObjectNode) jackson_mapper.readTree((String)k);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (positions.isEmpty()) {
		    positions.add(k);
		} else {
		    ObjectNode prev = null;
			try {
				prev = (ObjectNode) jackson_mapper.readTree(positions.getLast());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		    long dur = curr.get("t").asLong() - prev.get("t").asLong();
		    double dist = Geodesic.WGS84.Inverse(prev.get("lat").asDouble(), prev.get("lon").asDouble(), curr.get("lat").asDouble(), curr.get("lon").asDouble(), GeodesicMask.DISTANCE).s12;
            //dur secs 
		    if (dur > msp || dist > md) {
		        positions.add(k);
		    } else {
		        double course_diff = Math.abs(curr.get("course").asDouble() - prev.get("course").asDouble());
		        double speed_diff = Math.abs(curr.get("speed").asDouble() - prev.get("speed").asDouble());
		        //speed diff		
		        if (course_diff > min_course_diff || speed_diff > min_knots_diff) {
		            positions.add(k);
		        }
		    }
		}
    }

    public Object estimate(Object k) {
        return positions;
    }

    public Synopsis merge(Synopsis sk) {
        return null;
    }

	@Override
	public Estimation estimate(Request rq) {
		// TODO Auto-generated method stub
		return new Estimation(rq, positions, Integer.toString(rq.getUID()));
	}

}
