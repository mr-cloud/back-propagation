package org.mlgb.storm.back_propagation.topology.spout;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;

import java.util.Properties;

import org.apache.storm.spout.SchemeAsMultiScheme;
import org.mlgb.storm.back_propagation.topology.Keys;

/**
 * @author Leo
 */
public class SpoutBuilder {
	
	public Properties configs = null;
	
	public SpoutBuilder(Properties configs) {
		this.configs = configs;
	}
	public KafkaSpout buildKafkaSpout() {
		BrokerHosts hosts = new ZkHosts(configs.getProperty(Keys.KAFKA_ZOOKEEPER));
		String topic = configs.getProperty(Keys.KAFKA_TOPIC);
		String zkRoot = configs.getProperty(Keys.KAFKA_ZKROOT);
		String groupId = configs.getProperty(Keys.KAFKA_CONSUMERGROUP);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, groupId);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		//spoutConfig.ignoreZkOffsets = true;// read from head for each deployment or restart.
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		return kafkaSpout;
	}
}
