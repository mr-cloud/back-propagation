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
/**
 * parse varieties of data from kafka spout.
 * @author Leo
 *
 */
public class SplitterBolt extends BaseRichBolt{
	private static final long serialVersionUID = -4094707939635564788L;

	private String datasetType = "";
	private String outputField = "";
	private OutputCollector collector;
	private int latencyInMillis = 0;
	
    public SplitterBolt(String datasetType, String outputField) {
		// TODO Auto-generated constructor stub
    	this.datasetType = datasetType;
    	this.outputField = outputField;
	}

    public SplitterBolt(String datasetType, String outputField, int latencyInMillis) {
		// TODO Auto-generated constructor stub
    	this(datasetType, outputField);
    	this.latencyInMillis = latencyInMillis;
	}

	@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
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
        LatencySimulator.simulate(this.latencyInMillis);
		if(this.datasetType.equalsIgnoreCase(Keys.WIKIPEDIA_SAMPLE)
				|| this.datasetType.equalsIgnoreCase(Keys.WIKIPEDIA)
				|| this.datasetType.equalsIgnoreCase(Keys.ZIPF1)
				|| this.datasetType.equalsIgnoreCase(Keys.ZIPF2)){
	        String tokens[] = input.getString(0).split(" ");
			//	        String tokens[] = tuple.getString(0).split("\\s+");
	        if(tokens.length >= 2 && !StringUtils.isBlank(tokens[1])){
	        	collector.emit(new Values(tokens[1]));
	        }
		}
		this.collector.ack(input);
	}
}
