package org.mlgb.storm.back_propagation.metrics;

import java.util.ArrayList;
import java.util.List;
/**
 * loads stats helper.
 * @author Leo
 *
 */
public class LoadAuxer {
//	public  long interval = 10 * 60;//10 min
	public  long interval = 3 * 60;//3 min
	public  List<Long> loads = new ArrayList<>();
	public  long currentTs;
	public  long currentLoad = 0;
	public long totalLoad;
	public long startTs;
	public long costTime;//second as unit
	public long latestWorkingTs;
	public LoadAuxer(long timestamp) {
		// TODO Auto-generated constructor stub
		this.currentTs = timestamp;
		this.startTs = timestamp;
	}
	public void flushTailLoad(){
		this.totalLoad = this.currentLoad;
		this.costTime = (this.latestWorkingTs - this.startTs) == 0
				? this.interval: this.latestWorkingTs - this.startTs;
	}
	public void updateStats(long timestamp, long load) {
		// TODO Auto-generated method stub
		if(load != 0)
			this.latestWorkingTs = timestamp;
		this.currentLoad += load;
		if(timestamp - this.currentTs >= this.interval){
			this.loads.add(this.currentLoad);
			this.currentTs = timestamp;
		}
	}
}
