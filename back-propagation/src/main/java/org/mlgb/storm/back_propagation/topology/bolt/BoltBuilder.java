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
		String outputField = Keys.SPLITTER_BOLT_OUTPUTFIELD;
		int latencyInMillis = Integer.parseInt(this.configs.getProperty(Keys.SPLITTER_LATENCY_IN_MILLIS));
		return new SplitterBolt(datasetType, outputField, latencyInMillis);
	}
	
	public SkewedSourceBolt buildSkewedSource(){
		String outputField = Keys.SPLITTER_BOLT_OUTPUTFIELD;
		return new SkewedSourceBolt(outputField);
	}
	
	public CounterBolt buildCounter(){
		String outputField1 = Keys.COUNTER_BOLT_OUTPUTFIELD1;
		String outputField2 = Keys.COUNTER_BOLT_OUTPUTFIELD2;
		String metricName = this.configs.getProperty(Keys.METRIC_NAME);
		int metricTimeBucketSizeInSecs = Integer.parseInt(this.configs.getProperty(Keys.METRIC_TIME_BUCKET_SIZE_IN_SECS));
		int latencyInMillis = Integer.parseInt(this.configs.getProperty(Keys.LATENCY_IN_MILLIS));
		String loadOutputField = Keys.COUNTER_BOLT_LOAD_OUTPUT_FIELD;
		int calibrationTickFrequencySeconds = Integer.parseInt(this.configs.getProperty(Keys.CALIBRATION_TICK_FREQUENCY_SECONDS));
		return new CounterBolt(outputField1, outputField2, metricName, metricTimeBucketSizeInSecs, latencyInMillis, loadOutputField, calibrationTickFrequencySeconds);
	}
	
	public Tinker buildTinker(){
		String outputField = Keys.TINKER_OUTPUTFIELD;
		int workers = Integer.parseInt(this.configs.getProperty(Keys.NUM_WORKERS));
		return new Tinker(outputField, workers);
	}
	public AggregatorBolt buildAggregator(){
		String keyField = Keys.COUNTER_BOLT_OUTPUTFIELD1;
		String valueField = Keys.COUNTER_BOLT_OUTPUTFIELD2;
		String metricName = this.configs.getProperty(Keys.AGGREGATOR_METRIC_NAME);
		int metricTimeBucketSizeInSecs = Integer.parseInt(this.configs.getProperty(Keys.AGGREGATOR_METRIC_TIME_BUCKET_SIZE_IN_SECS));
		return new AggregatorBolt(keyField, valueField, metricName, metricTimeBucketSizeInSecs);
	}
}
