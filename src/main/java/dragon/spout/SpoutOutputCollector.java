package dragon.spout;

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
import dragon.topology.base.Collector;
import dragon.topology.base.IRichSpout;
import dragon.tuple.Tuple;
import dragon.tuple.Values;


public class SpoutOutputCollector extends Collector {
	private Log log = LogFactory.getLog(SpoutOutputCollector.class);
	private IRichSpout iRichSpout;
	
	public SpoutOutputCollector(LocalCluster localCluster,IRichSpout iRichSpout) {
		super(localCluster,(Integer)localCluster.getConf().get(Config.DRAGON_OUTPUT_BUFFER_SIZE),
			localCluster.getPersistanceDir()+"/"+iRichSpout.getComponentId()+"_"+iRichSpout.getTaskId());
		this.iRichSpout=iRichSpout;
	}
	
	public List<Integer> emit(Tuple anchorTuple, Values values){
		return emit(values);
	}
	
	public List<Integer> emit(String streamId,Tuple anchorTuple, Values values){
		return emit(streamId,values);
	}
	
	public List<Integer> emit(Values values){
		return emit(Constants.DEFAULT_STREAM,values);
	}
	
	public synchronized List<Integer> emit(String streamId,Values values){
		List<Integer> receivingTaskIds = new ArrayList<Integer>();
		Tuple tuple = new Tuple(iRichSpout.getOutputFieldsDeclarer().fields,values);
		for(String componentId : localCluster.getTopology().topology.get(iRichSpout.getComponentId()).keySet()) {
			HashMap<String,HashSet<CustomStreamGrouping>> component = 
					localCluster.getTopology().topology.get(iRichSpout.getComponentId()).get(componentId);
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
}
