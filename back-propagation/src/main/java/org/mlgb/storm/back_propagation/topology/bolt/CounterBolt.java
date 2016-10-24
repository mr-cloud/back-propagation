package org.mlgb.storm.back_propagation.topology.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.mlgb.storm.back_propagation.service.LatencySimulator;


public class CounterBolt extends BaseRichBolt{
    private static final long serialVersionUID = -2350373680379322599L;
    private Map<String, Integer> counts = new HashMap<String, Integer>();
    private static final int DEFAULT_TICK_FREQUENCY_SECONDS = 10;
    private String outputField1 = "";
    private String outputField2 = "";
	private transient CountMetric countMetric;
	private String metricName = "";
	private int metricTimeBucketSizeInSecs = 10;
	private int latencyInMillis = 0;
	private OutputCollector collector;
	
    public CounterBolt(String outputField1, String outputField2, String metricName, int metricTimeBucketSizeInSecs, int latencyInMillis) {
		// TODO Auto-generated constructor stub
    	this.outputField1 = outputField1;
    	this.outputField2 = outputField2;
    	this.metricName = metricName;
    	this.metricTimeBucketSizeInSecs = metricTimeBucketSizeInSecs;
    	this.latencyInMillis = latencyInMillis;
	}

    
	@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(this.outputField1, this.outputField2));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, DEFAULT_TICK_FREQUENCY_SECONDS);
        return conf;
    }

    private static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    private void emit(OutputCollector collector) {
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            String str = entry.getKey();
            Integer count = entry.getValue();
            collector.emit(new Values(str, count));
        }
    }

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.countMetric = new CountMetric();
		context.registerMetric(this.metricName, this.countMetric, this.metricTimeBucketSizeInSecs);
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
        if (isTickTuple(input)) {
            emit(collector);
            counts = new HashMap<String, Integer>();
        } else {
            LatencySimulator.simulate(this.latencyInMillis);
            String word = input.getString(0);
            if (!word.isEmpty()) {
                Integer count = counts.get(word);
                if (count == null) {
                    count = 0;
                }
                count++;
                counts.put(word, count);
            }
            this.countMetric.incr();
        }
        this.collector.ack(input);
	}
}
