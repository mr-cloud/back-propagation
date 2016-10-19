package org.mlgb.storm.back_propagation;

import java.util.Properties;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.mlgb.storm.back_propagation.spout.SpoutBuilder;

public class BackProgationTopologyBuilder {

	public static StormTopology build(Properties configs) {
		// TODO Auto-generated method stub
		TopologyBuilder builder = new TopologyBuilder();
		SpoutBuilder spoutBuilder = new SpoutBuilder(configs);
		
		//set the kafkaSpout to topology
		//parallelism-hint for kafkaSpout - defines number of executors/threads to be spawn per container
		int kafkaSpoutCount = Integer.parseInt(configs.getProperty(Keys.KAFKA_SPOUT_COUNT));
		builder.setSpout(configs.getProperty(Keys.KAFKA_SPOUT_ID), spoutBuilder.buildKafkaSpout(), kafkaSpoutCount);
		
		return builder.createTopology();
	}

}
