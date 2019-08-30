package dragon.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.Constants;
import dragon.LocalCluster;
import dragon.NetworkTask;
import dragon.grouping.CustomStreamGrouping;
import dragon.topology.base.IRichBolt;
import dragon.tuple.Tuple;
import dragon.tuple.Values;
import dragon.utils.DurableCircularBuffer;


public class OutputCollector {
	private Log log = LogFactory.getLog(OutputCollector.class);
	private DurableCircularBuffer<NetworkTask> outputQueue;
	private LocalCluster localCluster;
	private IRichBolt iRichBolt;
	
	public OutputCollector(LocalCluster localCluster,IRichBolt iRichBolt) {
		outputQueue=new DurableCircularBuffer<NetworkTask>(
				(Integer)localCluster.getConf().get(Config.DRAGON_OUTPUT_BUFFER_SIZE),
				localCluster.getPersistanceDir()+"/"+iRichBolt.getComponentId()+"_"+iRichBolt.getTaskId());
		this.localCluster=localCluster;
		this.iRichBolt=iRichBolt;
	}
	
	public synchronized List<Integer> emit(Tuple anchorTuple, Values values){
		return emit(values);
	}
	
	public synchronized List<Integer> emit(String streamId,Tuple anchorTuple, Values values){
		return emit(streamId,values);
	}
	
	public synchronized List<Integer> emit(String streamId,Values values){
		List<Integer> receivingTaskIds = new ArrayList<Integer>();
		Tuple tuple = new Tuple(iRichBolt.getOutputFieldsDeclarer().fields,values);
		for(String componentId : localCluster.getTopology().topology.get(iRichBolt.getComponentId()).keySet()) {
			HashMap<String,HashSet<CustomStreamGrouping>> component = 
					localCluster.getTopology().topology.get(iRichBolt.getComponentId()).get(componentId);
			HashSet<CustomStreamGrouping> stream = component.get(streamId);
			for(CustomStreamGrouping grouping : new ArrayList<CustomStreamGrouping>(stream)) {
				List<Integer> taskIds = grouping.chooseTasks(0, values);
				receivingTaskIds.addAll(taskIds);
				try {
					outputQueue.put(new NetworkTask(tuple,new HashSet<Integer>(taskIds),componentId));
					localCluster.outputPending(this);
				} catch (InterruptedException e) {
					log.error("failed to emit tuple: "+e.toString());
				}
			}
		}
		return receivingTaskIds;
	}
	
	public synchronized List<Integer> emit(Values values){
		return emit(Constants.DEFAULT_STREAM,values);
	}
	
	public void ack(Tuple tuple) {
		
	}
	
	public DurableCircularBuffer<NetworkTask> getQueue(){
		return outputQueue;
	}
}
