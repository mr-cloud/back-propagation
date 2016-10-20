package org.mlgb.storm.back_propagation.topology.bolt;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SkewedSourceBolt extends BaseBasicBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4410070145947171583L;

	private String outputField;
	
	public SkewedSourceBolt(String outputField){
		this.outputField = outputField;
	}
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String word = input.getString(0);
		if(!StringUtils.isBlank(word)){
			collector.emit(new Values(word));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(this.outputField));
	}

}
