package org.mlgb.storm.back_propagation;

/**
 * This is an utility class. It contains the keys that should be present in the input config-file
 * @author Leo
 */
public class Keys {
		
	//kafka spout
	public static final String KAFKA_SPOUT_ID = "kafka-spout";
	public static final String KAFKA_ZOOKEEPER               = "kafka.zookeeper";
	public static final String KAFKA_TOPIC              = "kafa.topic";
	public static final String KAFKA_ZKROOT                    = "kafka.zkRoot";
	public static final String KAFKA_CONSUMERGROUP     = "kafka.consumer.group";
	public static final String KAFKA_SPOUT_COUNT          = "kafka-spout.count";
	
	//splitter
	public static final String SPLITTER_BOLT_ID = "splitter-bolt";
	public static final String SPLITTER_BOLT_COUNT = "splitter-bolt.count";
	
	//counter
	public static final String COUNTER_BOLT_ID = "counter-bolt";
	public static final String COUNTER_BOLT_COUNT = "counter-bolt.count";
	
	//aggregator
	public static final String AGGREGATOR_BOLT_ID = "aggregator-bolt";
	public static final String AGGREGATOR_BOLT_COUNT = "aggregator-bolt.count";
	
	//storm workers number
	public static final String NUM_WORKERS = "NUM_WORKERS";
	
	//message timeout
	public static final String TIMEOUT = "TIMEOUT";
	
	//eventloggers number
	public static final String NUM_EVENTLOGGERS = "NUM_EVENTLOGGERS";
	
	//dataset type
	public static final String WIKIPEDIA_SAMPLE = "sampledWP";
	public static final String WIKIPEDIA = "WP";
	//output fields
	public static final String SPLITTER_BOLT_OUTPUTFIELD = "word";
	public static final String COUNTER_BOLT_OUTPUTFIELD1 = "word";
	public static final String COUNTER_BOLT_OUTPUTFIELD2 = "count";
	
	
}
