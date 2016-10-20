package org.mlgb.storm.back_propagation.topology.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class AggregatorBolt extends BaseBasicBolt{
    private static final long serialVersionUID = -1410983886447378438L;
    private Map<String, Integer> counts = new HashMap<String, Integer>();

    private String keyField = "";
    private String valueField = "";
    
    public AggregatorBolt(String keyField, String valueField) {
		// TODO Auto-generated constructor stub
    	this.keyField = keyField;
    	this.valueField = valueField;
	}

	@Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getStringByField(this.keyField);
        if (!word.isEmpty()) {
            Integer delta_count = tuple.getIntegerByField(this.valueField);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count = count + delta_count;
            counts.put(word, count);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
