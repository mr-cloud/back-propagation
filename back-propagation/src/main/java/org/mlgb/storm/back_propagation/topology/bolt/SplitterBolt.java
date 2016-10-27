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
import org.mlgb.storm.back_propagation.service.LatencySimulator;
import org.mlgb.storm.back_propagation.topology.Keys;

public class SplitterBolt extends BaseRichBolt{
	private static final long serialVersionUID = -4094707939635564788L;

	private String datasetType = "";
	private String outputField1 = "";
	private String outputField2 = "";
	private OutputCollector collector;
	private int latencyInMillis = 0;

	public SplitterBolt(String datasetType, String outputField1, String outputField2, int latencyInMillis) {
		// TODO Auto-generated constructor stub
    	this.datasetType = datasetType;
    	this.outputField1 = outputField1;
    	this.outputField2 = outputField2;
    	this.latencyInMillis = latencyInMillis;
	}

	@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(this.outputField1, this.outputField2));
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
        LatencySimulator.simulate(this.latencyInMillis);
		if(this.datasetType.equalsIgnoreCase(Keys.LJ)){
			String tokens[] = input.getString(0).split("\\s+");
			if(tokens.length >= 2 && !StringUtils.isBlank(tokens[0]) && !StringUtils.isBlank(tokens[1])){
				collector.emit(new Values(tokens[0], tokens[1]));
			}
		}
		this.collector.ack(input);
	}
}
