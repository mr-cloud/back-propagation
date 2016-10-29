package org.mlgb.storm.back_propagation.topology.bolt;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
/**
 * periodical calibration signal.
 * @author Leo
 *
 */
public class CalibrationSignal implements Serializable{

	public Map<Integer, Long> tasksLoad = new HashMap<>();
	
	public CalibrationSignal(Map<Integer, Long> tasksLoad) {
		// TODO Auto-generated constructor stub
		for(Entry<Integer, Long> en: tasksLoad.entrySet()){
			this.tasksLoad.put(en.getKey().intValue(), en.getValue().longValue());
		}
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


}
