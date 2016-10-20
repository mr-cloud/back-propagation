package org.mlgb.storm.back_propagation.topology;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.metric.LoggingMetricsConsumer;
/**
 * create a topology from builder, configure the topology and submit it to Storm cluster.
 * @author Leo
 *
 */
public class RemoteTopologyRunner {
  public static void main(String[] args) throws Exception {
    if (args.length <= 0 || args.length > 3 || StringUtils.isBlank(args[0])) {
      System.out.println("Error deploying topology.");
      System.out.println("Usage: <topology name> debug(optional)");
      System.out.println("Please provide correct command-line arguments and try again.");
      System.exit(1);
    }

    String topologyName = args[0];
	String configFile;
	if((args.length == 2 && !"debug".equalsIgnoreCase(args[1]))
			|| args.length == 3){
		configFile = args[1];
	}
	else{
		System.out.println("Missing input : config file location, using default");
		configFile = Keys.DEFAULT_CONFIG;		
	}
	Properties configs = loadConfigProperties(configFile);
    Config config = createConfig(shouldRunInDebugMode(args), configs);
    StormSubmitter.submitTopology(topologyName, config, BackProgationTopologyBuilder.build(configs));
  }

  private static Properties loadConfigProperties(String configFile) {
	// TODO Auto-generated method stub
		Properties configs = new Properties();
		try {
			configs.load(RemoteTopologyRunner.class.getResourceAsStream("/default_config.properties"));
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(0);
		}
		return configs;
}

private static boolean shouldRunInDebugMode(String[] args) {
    return args.length > 1 && "debug".equalsIgnoreCase(args[args.length-1]);
  }

  private static Config createConfig(Boolean debug, Properties configs) {
    Config config = new Config();
    config.setDebug(debug);
    config.setMessageTimeoutSecs(Integer.parseInt(configs.getProperty(Keys.TIMEOUT)));
    config.setNumWorkers(Integer.parseInt(configs.getProperty(Keys.NUM_WORKERS)));
    config.setNumAckers(Integer.parseInt(configs.getProperty(Keys.NUM_WORKERS)));
    config.registerMetricsConsumer(LoggingMetricsConsumer.class, Integer.parseInt(configs.getProperty(Keys.NUM_METRICS_CONSUMER)));
    config.setNumEventLoggers(Integer.parseInt(configs.getProperty(Keys.NUM_EVENTLOGGERS)));
    return config;
  }
}
