package org.mlgb.storm.back_propagation.topology.bolt;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.mlgb.storm.back_propagation.topology.Keys;

public class SplitterBolt extends BaseBasicBolt{
	private static final long serialVersionUID = -4094707939635564788L;

	private String datsetType = "";
	private String outputField = "";
	
    public SplitterBolt(String datasetType, String outputField) {
		// TODO Auto-generated constructor stub
    	this.datsetType = datasetType;
    	this.outputField = outputField;
	}

	@Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
		if(this.datsetType.equalsIgnoreCase(Keys.WIKIPEDIA_SAMPLE)
				|| this.datsetType.equalsIgnoreCase(Keys.WIKIPEDIA)){
	        String tokens[] = tuple.getString(0).split(" ");
			//	        String tokens[] = tuple.getString(0).split("\\s+");
	        if(tokens.length >= 2 && !StringUtils.isBlank(tokens[1])){
	        	collector.emit(new Values(tokens[1]));
	        }
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(this.outputField));
    }
}
