package org.mlgb.storm.back_propagation.topology.bolt;

import java.io.Serializable;
/**
 * periodical calibration signal.
 * @author Leo
 *
 */
public class CalibrationSignal implements Serializable{

	public CalibrationSignal(int taskId, long load) {
		// TODO Auto-generated constructor stub
		this.taskId = taskId;
		this.load = load;
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public int taskId;
	public long load;

}
