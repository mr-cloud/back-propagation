package org.mlgb.storm.back_propagation.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.metric.api.IMetric;
/**
 * customized metrics collector.
 * @author Leo
 *
 */
public class WordCountMetric implements IMetric{
	private Map<String, Integer> counts = new HashMap<>();
	@Override
	public Object getValueAndReset() {
		// TODO Auto-generated method stub
		Map<String, Integer> stats = new HashMap<>();
		for(Entry<String, Integer> en: this.counts.entrySet()){
			stats.put(en.getKey(), en.getValue());
		}
		this.counts.clear();
		return stats;
	}

	public void updateCounts(Map<String, Integer> counts){
		for(Entry<String, Integer> en: counts.entrySet()){
			this.counts.put(en.getKey(), en.getValue());
		}
	}
}
