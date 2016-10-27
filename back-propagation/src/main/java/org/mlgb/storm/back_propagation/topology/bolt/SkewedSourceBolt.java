package org.mlgb.storm.back_propagation.topology.bolt;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SkewedSourceBolt extends BaseRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4410070145947171583L;

	private String outputField;
	private OutputCollector collector;
	
	public SkewedSourceBolt(String outputField){
		this.outputField = outputField;
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(this.outputField));
	}
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String to = input.getString(1);
		if(!StringUtils.isBlank(to)){
			collector.emit(new Values(to));
		}
		this.collector.ack(input);
	}

}
