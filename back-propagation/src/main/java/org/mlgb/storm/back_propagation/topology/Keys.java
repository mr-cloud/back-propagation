package org.mlgb.storm.back_propagation.topology;

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
	//splitter latency
	public static final String SPLITTER_LATENCY_IN_MILLIS = "SPLITTER_LATENCY_IN_MILLIS";

	//skewed source
	public static final String SKEWED_DATA_BOLT_ID = "skewed-source-bolt";
	public static final String SKEWED_DATA_BOLT_COUNT = "skewed-source-bolt.count";
	
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
	public static final String LJ = "LJ";

	//output fields
	public static final String SPLITTER_BOLT_OUTPUTFIELD1 = "from";
	public static final String SPLITTER_BOLT_OUTPUTFIELD2 = "to";
	public static final String COUNTER_BOLT_OUTPUTFIELD1 = "word";
	public static final String COUNTER_BOLT_OUTPUTFIELD2 = "count";
	public static final String COUNTER_BOLT_LOAD_OUTPUT_FIELD = "load";

	//metrics consumer number
	public static final String NUM_METRICS_CONSUMER = "NUM_METRICS_CONSUMER";
	
	//counter metric
	public static final String METRIC_NAME = "METRIC_NAME";
	public static final String METRIC_TIME_BUCKET_SIZE_IN_SECS = "METRIC_TIME_BUCKET_SIZE_IN_SECS";
	//counter latency
	public static final String LATENCY_IN_MILLIS = "LATENCY_IN_MILLIS";
	
	//default configuration file name
	public static final String DEFAULT_CONFIG = "default_config.properties";
	
	//topology name
	public static final String TOPOLOGY_NAME = "BACK-PROPAGATION";
	
	//aggregator metric
	public static final String AGGREGATOR_METRIC_NAME = "AGGREGATOR_METRIC_NAME";
	public static final String AGGREGATOR_METRIC_TIME_BUCKET_SIZE_IN_SECS = "AGGREGATOR_METRIC_TIME_BUCKET_SIZE_IN_SECS";
	
	//stats sample rate
	public static final String TOPOLOGY_STATS_SAMPLE_RATE = "TOPOLOGY_STATS_SAMPLE_RATE";
	//acker number
	public static final String NUM_ACKERS = "NUM_ACKERS";
	//backp-ressure
	public static final String TOPOLOGY_BACKPRESSURE_ENABLE = "TOPOLOGY_BACKPRESSURE_ENABLE";
	
	//stream of counter
	public static final String COUNTER_BOLT_DATA_STREAM = "COUNTER_BOLT_DATA_STREAM";
	public static final String COUNTER_BOLT_BP_STREAM = "COUNTER_BOLT_BP_STREAM";
	
	//calibration frequency
	public static final String CALIBRATION_TICK_FREQUENCY_SECONDS = "CALIBRATION_TICK_FREQUENCY_SECONDS";
	
}
