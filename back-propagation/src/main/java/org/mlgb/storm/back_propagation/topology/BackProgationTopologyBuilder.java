package org.mlgb.storm.back_propagation.topology;

import java.util.Properties;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.mlgb.storm.back_propagation.topology.bolt.BoltBuilder;
import org.mlgb.storm.back_propagation.topology.spout.SpoutBuilder;

/**
 * build topology's nodes and edges.
 * @author Leo
 *
 */
public class BackProgationTopologyBuilder {

	public static StormTopology build(Properties configs) {
		// TODO Auto-generated method stub
		TopologyBuilder builder = new TopologyBuilder();
		SpoutBuilder spoutBuilder = new SpoutBuilder(configs);
		BoltBuilder boltBuilder = new BoltBuilder(configs);
		
		//set the kafkaSpout to topology
		//parallelism-hint for kafkaSpout - defines number of executors/threads to be spawn per container
		int kafkaSpoutCount = Integer.parseInt(configs.getProperty(Keys.KAFKA_SPOUT_COUNT));
		builder.setSpout(configs.getProperty(Keys.KAFKA_SPOUT_ID), spoutBuilder.buildKafkaSpout(), kafkaSpoutCount);
		
		//set the splitter to topology
		int splitterBoltCount = Integer.parseInt(configs.getProperty(Keys.SPLITTER_BOLT_COUNT));
		builder.setBolt(configs.getProperty(Keys.SPLITTER_BOLT_ID), boltBuilder.buildSplitter(), splitterBoltCount)
			.shuffleGrouping(configs.getProperty(Keys.KAFKA_SPOUT_ID));
		
		//set the skewed source to topology
		int skewedSourceBoltCount = Integer.parseInt(configs.getProperty(Keys.SKEWED_DATA_BOLT_COUNT));
		builder.setBolt(configs.getProperty(Keys.SKEWED_DATA_BOLT_ID), boltBuilder.buildSkewedSource(), skewedSourceBoltCount)
			.fieldsGrouping(configs.getProperty(Keys.SPLITTER_BOLT_ID), new Fields(Keys.SPLITTER_BOLT_OUTPUTFIELD1));
		
		//set the counter to topology
		int counterBoltCount = Integer.parseInt(configs.getProperty(Keys.COUNTER_BOLT_COUNT));
		builder.setBolt(configs.getProperty(Keys.COUNTER_BOLT_ID), boltBuilder.buildCounter(), counterBoltCount)
			.customGrouping(configs.getProperty(Keys.SKEWED_DATA_BOLT_ID), new PartialKeyGrouping());
		
		//set aggregator to topology
/*		int aggregatorBoltCount = Integer.parseInt(configs.getProperty(Keys.AGGREGATOR_BOLT_COUNT));
		builder.setBolt(configs.getProperty(Keys.AGGREGATOR_BOLT_ID), boltBuilder.buildAggregator(), aggregatorBoltCount)
			.fieldsGrouping(configs.getProperty(Keys.COUNTER_BOLT_ID), new Fields(Keys.COUNTER_BOLT_OUTPUTFIELD1));
*/		
		return builder.createTopology();
	}

}
