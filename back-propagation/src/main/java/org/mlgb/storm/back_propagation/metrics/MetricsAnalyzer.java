package org.mlgb.storm.back_propagation.metrics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

/**
 * analyze the metrics log and generate stats for plotting.
 * @author Leo
 *
 */
public class MetricsAnalyzer {
	public static void main(String[] args){
		File metricsLog;
		if(args.length != 0){
			metricsLog = new File(args[0]);
		}
		else{
			metricsLog = new File("C:\\Users\\MrCloud\\Desktop\\metrics-logs");
		}
		if(!metricsLog.exists()){
			System.out.println("file or directory does not exist!");
			return;
		}
		else{
			File metricsResultsDir = new File(metricsLog.getParent() + "/" + "metrcis-results");
			if(!metricsResultsDir.exists()){
				if(!metricsResultsDir.mkdir()){
					System.out.println("cannot mkdir for metrics results.");
				}
			}
			else if(!metricsResultsDir.isDirectory()){
				if(!metricsResultsDir.delete()){
					System.out.println("cannot delete existing file named 'metrcis-results'.");
				}
				if(!metricsResultsDir.mkdir()){
					System.out.println("cannot mkdir for metrics results.");
				}
			}
			else
				;
			if(metricsLog.isDirectory()){
				File[] logs = metricsLog.listFiles();
				for(File log: logs){
					try {
						analizeLog(metricsResultsDir, log);
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			else{
				try {
					analizeLog(metricsResultsDir, metricsLog);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		System.out.println("\ntask finished.");
	}

	private static void analizeLog(File metricsResultsDir, File log) throws FileNotFoundException {
		// TODO Auto-generated method stub
		System.out.println("\nanalyzing " + log.getName() + "...");
		String[] nameSegments = log.getName().trim().split("-");
		if(nameSegments.length != 5){
			System.out.println("can not parse log name!");
			return;
		}
		String grouping = nameSegments[0];
		String dataset = nameSegments[1];
		String sources = nameSegments[2];
		String workers = nameSegments[3];
		String delay = nameSegments[4];
		
		Map<Long, LoadAuxer> loadMap = new HashMap<>();
		BufferedReader bfr = new BufferedReader(new FileReader(log));
		String line;
		try {
//			long loadTest = 0;
			while((line=bfr.readLine()) != null){
				line = line.trim();
				String[] items = line.split("\t");
				if(items.length != 5 || !"keys-per-bucket".equalsIgnoreCase(items[3].trim())){
					continue;
				}
				long timestamp = Long.parseLong(items[0].trim().split("\\s+")[3]);
				long taskId = Long.parseLong(items[2].trim().split(":")[0]);
				long load = Long.parseLong(items[4].trim());
//				loadTest += load;
				LoadAuxer loadAuxer = loadMap.get(taskId);
				if(loadAuxer == null){
					loadAuxer = new LoadAuxer(timestamp);
				}
				loadAuxer.updateStats(timestamp, load);
				loadMap.put(taskId, loadAuxer);
			}
			bfr.close();
//			System.out.println("test load in toal: " + loadTest);
			long loadSum = 0;
			long maxLoad = 0;
			long maxCostTime = 0;
			long mostTimeConsumingTaskSlotLength = 0;
			for(LoadAuxer tmp: loadMap.values()){
				tmp.flushTailLoad();
				if(tmp.loads.size() > mostTimeConsumingTaskSlotLength){
					mostTimeConsumingTaskSlotLength = tmp.loads.size();
				}
				loadSum += tmp.totalLoad;
				if(tmp.totalLoad > maxLoad){
					maxLoad = tmp.totalLoad;
				}
				if(tmp.costTime > maxCostTime)
					maxCostTime = tmp.costTime;
			}
			if(loadMap.size() ==0  || loadSum == 0){
				System.out.println("this log file contains nothing related to metircs!");
				bfr.close();
				return;
			}
			double imbalance = ((double)maxLoad - ((double)loadSum)/loadMap.size())/loadSum;
			long throughput = loadSum/maxCostTime;
			List<Double> imbalances = new ArrayList<>();
			for(int cnt = 0; cnt < mostTimeConsumingTaskSlotLength; cnt++){
				long curLoadSum = 0;
				long curMaxLoad = 0;
				for(LoadAuxer loadAuxer: loadMap.values()){
					if(cnt < loadAuxer.loads.size()){
						long curLoad = loadAuxer.loads.get(cnt);
						curLoadSum += curLoad;
						if(curLoad > curMaxLoad)
							curMaxLoad = curLoad;
					}
				}
				imbalances.add(((double)curMaxLoad - ((double)curLoadSum)/loadMap.size())/curLoadSum);
			}
			printStats(loadSum, maxLoad, maxCostTime, imbalance, throughput, imbalances);
			
			persistStats(metricsResultsDir.getAbsolutePath() + "/" + log.getName(), dataset, sources, workers, grouping, delay, imbalance, throughput, imbalances);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	private static void persistStats(String metricsResultsFileName, String dataset, String sources, String workers, String grouping, String delay,
			double imbalance, long throughput, List<Double> imbalances) {
		// TODO Auto-generated method stub
		JSONObject obj = new JSONObject();
		obj.put("dataset", dataset);
		obj.put("sources", sources);
		obj.put("workers", workers);
		obj.put("grouping", grouping);
		obj.put("delay", delay);
		obj.put("imbalance", imbalance);
		obj.put("throughput", throughput);
		obj.put("imbalances", imbalances);
//		System.out.println("json string: " + obj.toJSONString());
//		System.out.println("json to string: " + obj.toString());
		File metricsResult = new File(metricsResultsFileName);
		if(metricsResult.exists()){
			File backupFile = new File(metricsResult.getAbsolutePath() + "-backup");
			if(backupFile.exists()){
				backupFile.delete();
			}
			metricsResult.renameTo(backupFile);
		}
		try {
			BufferedWriter bfw = new BufferedWriter(new FileWriter(metricsResult));
			bfw.write(obj.toJSONString());
			bfw.flush();
			bfw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void printStats(long loadSum, long maxLoad, long maxCostTime, double imbalance, long throughput,
			List<Double> imbalances) {
		// TODO Auto-generated method stub
		DecimalFormat formatter = new DecimalFormat("0.######E0");
		System.out.println("***stats results***");
		System.out.println("load in total: " + loadSum
				+ "\n" + "maximal load of task: " + maxLoad
				+ "\n" + "time cost of job in minutes: " + maxCostTime/60
				+ "\n" + "throughput(keys/s): " + throughput
				+ "\n" + "imbalance ratio in total: " + formatter.format(imbalance));
/*		System.out.println("\nimbalance-time:");
		for(double imb: imbalances){
			System.out.println(formatter.format(imb));
		}*/
	}
}
