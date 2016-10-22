package org.mlgb.storm.back_propagation.topology.bolt;

import java.util.HashMap;
import java.util.Map;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.mlgb.storm.back_propagation.metrics.WordCountMetric;

public class AggregatorBolt extends BaseRichBolt{
    private static final long serialVersionUID = -1410983886447378438L;
    private Map<String, Integer> counts = new HashMap<String, Integer>();

    private String keyField = "";
    private String valueField = "";
    private OutputCollector collector;
    
	private transient WordCountMetric wordCountMetric;
	private String metricName = "";
	private int metricTimeBucketSizeInSecs;
	
    public AggregatorBolt(String keyField, String valueField, String metricName, int metricTimeBucketSizeInSecs) {
		// TODO Auto-generated constructor stub
    	this.keyField = keyField;
    	this.valueField = valueField;
    	this.metricName = metricName;
    	this.metricTimeBucketSizeInSecs = metricTimeBucketSizeInSecs;
	}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.wordCountMetric = new WordCountMetric();
		context.registerMetric(this.metricName, this.wordCountMetric, this.metricTimeBucketSizeInSecs);
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
        String word = input.getStringByField(this.keyField);
        if (!word.isEmpty()) {
            Integer delta_count = input.getIntegerByField(this.valueField);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count = count + delta_count;
            counts.put(word, count);
            this.wordCountMetric.updateCounts(this.counts);
        }
        this.collector.ack(input);
	}
}
