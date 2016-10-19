package org.mlgb.storm.back_propagation.bolt;

import java.util.Properties;

import org.mlgb.storm.back_propagation.Keys;


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
	
	public CounterBolt buildCounter(){
		String outputField1 = this.configs.getProperty(Keys.COUNTER_BOLT_OUTPUTFIELD1);
		String outputField2 = this.configs.getProperty(Keys.COUNTER_BOLT_OUTPUTFIELD2);
		return new CounterBolt(outputField1, outputField2);
	}
	
	public AggregatorBolt buildAggregator(){
		String keyField = this.configs.getProperty(Keys.COUNTER_BOLT_OUTPUTFIELD1);
		String valueField = this.configs.getProperty(Keys.COUNTER_BOLT_OUTPUTFIELD2);
		return new AggregatorBolt(keyField, valueField);
	}
}
