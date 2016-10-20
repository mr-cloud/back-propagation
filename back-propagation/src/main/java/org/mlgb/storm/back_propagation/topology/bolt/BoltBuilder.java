package org.mlgb.storm.back_propagation.topology.bolt;

import java.util.Properties;

import org.mlgb.storm.back_propagation.topology.Keys;


/**
 * @author Leo
 */
public class BoltBuilder {
	
	public Properties configs = null;
	
	public BoltBuilder(Properties configs) {
		this.configs = configs;
	}
	
	public SplitterBolt buildSplitter(){
		String datasetType = this.configs.getProperty(Keys.KAFKA_TOPIC);
		String outputField = this.configs.getProperty(Keys.SPLITTER_BOLT_OUTPUTFIELD);
		return new SplitterBolt(datasetType, outputField);
	}
	
	public SkewedSourceBolt buildSkewedSource(){
		String outputField = this.configs.getProperty(Keys.SPLITTER_BOLT_OUTPUTFIELD);
		return new SkewedSourceBolt(outputField);
	}
	
	public CounterBolt buildCounter(){
		String outputField1 = this.configs.getProperty(Keys.COUNTER_BOLT_OUTPUTFIELD1);
		String outputField2 = this.configs.getProperty(Keys.COUNTER_BOLT_OUTPUTFIELD2);
		String metricName = this.configs.getProperty(Keys.METRIC_NAME);
		int metricTimeBucketSizeInSecs = Integer.parseInt(this.configs.getProperty(Keys.METRIC_TIME_BUCKET_SIZE_IN_SECS));
		int latencyInMillis = Integer.parseInt(this.configs.getProperty(Keys.LATENCY_IN_MILLIS));
		return new CounterBolt(outputField1, outputField2, metricName, metricTimeBucketSizeInSecs, latencyInMillis);
	}
	
	public AggregatorBolt buildAggregator(){
		String keyField = this.configs.getProperty(Keys.COUNTER_BOLT_OUTPUTFIELD1);
		String valueField = this.configs.getProperty(Keys.COUNTER_BOLT_OUTPUTFIELD2);
		return new AggregatorBolt(keyField, valueField);
	}
}