package org.mlgb.storm.back_propagation.topology.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.mlgb.storm.back_propagation.topology.Keys;

public class Tinker extends BaseRichBolt{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private String outputField;
	private Map<Integer, Long> tasksLoad;
	private int tasksNum;
	
	public Tinker(String outputField, int tasksNum){
		this.outputField = outputField;
		this.tasksNum = tasksNum;
		this.tasksLoad = new HashMap<>();
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		if(Keys.COUNTER_BOLT_BP_STREAM.equalsIgnoreCase(input.getSourceStreamId())){
			int taskId = input.getSourceTask();
			long load = input.getLong(0);
			this.tasksLoad.put(taskId, load);
			if(this.tasksLoad.size() == this.tasksNum){
				this.collector.emit(new Values(new CalibrationSignal(this.tasksLoad)));
				this.tasksLoad.clear();
			}	
		}
		this.collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(this.outputField));
	}

}
