package org.mlgb.storm.back_propagation.topology;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.mlgb.storm.back_propagation.topology.bolt.CalibrationSignal;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
/**
 * partial key grouping with back propagation mechanism.
 * @author Leo
 *
 */
public class PKGWithBackPropagation implements CustomStreamGrouping, Serializable {
    private static final long serialVersionUID = -447379837314000353L;
    private List<Integer> targetTasks;//tasks id.
    private long[] targetTaskStats;// tasks load.
    /*
     * choices
     */
    private HashFunction h1 = Hashing.murmur3_128(13);
    private HashFunction h2 = Hashing.murmur3_128(17);

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
        targetTaskStats = new long[this.targetTasks.size()];
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
    	List<Integer> boltIds = new ArrayList<Integer>(1);
    	if(values.size() == 0)
    		return boltIds;
    	if(values.get(0) instanceof CalibrationSignal){
    		CalibrationSignal signal = (CalibrationSignal)(values.get(0));
    		for(int i = 0; i < this.targetTasks.size(); i++){
    			Long load = signal.tasksLoad.get(this.targetTasks.get(i).intValue());
    			this.targetTaskStats[i] = load.longValue();
    		}
    		return boltIds;
    	}
    	else{
    		String str = values.get(0).toString(); // assume key is the first field
            int firstChoice = (int) (Math.abs(h1.hashBytes(str.getBytes()).asLong()) % this.targetTasks.size());
            int secondChoice = (int) (Math.abs(h2.hashBytes(str.getBytes()).asLong()) % this.targetTasks.size());
            int selected = targetTaskStats[firstChoice] > targetTaskStats[secondChoice] ? secondChoice : firstChoice;
            boltIds.add(targetTasks.get(selected));
            targetTaskStats[selected]++;	
            return boltIds;	
    	}
    }
}
