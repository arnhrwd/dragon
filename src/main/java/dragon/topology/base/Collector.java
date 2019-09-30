package dragon.topology.base;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Constants;
import dragon.LocalCluster;
import dragon.grouping.AbstractGrouping;
import dragon.topology.DestComponentMap;
import dragon.topology.GroupingsSet;
import dragon.topology.StreamMap;
import dragon.tuple.Fields;
import dragon.tuple.NetworkTask;
import dragon.tuple.RecycleStation;
import dragon.tuple.Tuple;
import dragon.tuple.Values;
import dragon.utils.ComponentTaskBuffer;
import dragon.utils.NetworkTaskBuffer;



public class Collector {
	private Log log = LogFactory.getLog(Collector.class);
	//private NetworkTaskBuffer outputQueue;
	private final ComponentTaskBuffer outputQueues;
	private final LocalCluster localCluster;
	private final Component component;
	
	private boolean emitted;
	
	public Collector(Component component,LocalCluster localCluster,int bufSize) {
		this.component = component;
		this.localCluster=localCluster;
		outputQueues=new ComponentTaskBuffer(bufSize);
		DestComponentMap destComponentMap = localCluster.getTopology().getDestComponentMap(component.getComponentId());
		if(destComponentMap!=null) {
			for(String destId : destComponentMap.keySet()) {
				for(String streamId : destComponentMap.get(destId).keySet() ) {
					outputQueues.create(destId, streamId);
				}
			}
		}
	}
	
	public NetworkTaskBuffer getQueue(String componentId, String streamId){
		return outputQueues.get(componentId).get(streamId);
	}
	
	public ComponentTaskBuffer getComponentTaskBuffer() {
		return outputQueues;
	}
	
	@Deprecated
	public synchronized List<Integer> emit(Tuple anchorTuple, Values values){
		return emit(values);
	}
	
	@Deprecated
	public synchronized List<Integer> emit(String streamId,Tuple anchorTuple, Values values){
		return emit(streamId,values);
	}
	
	public synchronized List<Integer> emit(Values values){
		return emit(Constants.DEFAULT_STREAM,values);
	}
	
	public synchronized List<Integer> emit(String streamId,Values values){
		List<Integer> receivingTaskIds = new ArrayList<Integer>();
		if(component.isClosed()) {
			log.error("spontaneous tuple emission after close, topology may not terminate properly");
			return receivingTaskIds;
		}
		Fields fields = component.getOutputFieldsDeclarer().getFields(streamId);
		if(fields==null) {
			localCluster.setShouldTerminate("no fields have been declared for ["+
					component.getComponentId()+"] on stream ["+streamId+
					"] however it is attempting to emit on that stream");
		}
		if(values.size()!=fields.getFieldNames().length) {
			localCluster.setShouldTerminate("the number of values in ["+values+
					"] does not match the number of fields ["+
					fields.getFieldNamesAsString()+"]");
		}
		Tuple tuple = RecycleStation.getInstance()
				.getTupleRecycler(fields.getFieldNamesAsString())
				.newObject();
		tuple.setValues(values);
		tuple.setSourceComponent(component.getComponentId());
		tuple.setSourceTaskId(component.getTaskId());
		tuple.setSourceStreamId(streamId);
		component.incEmitted(1); // for metrics
		for(String componentId : localCluster.getTopology().getTopology().get(component.getComponentId()).keySet()) {
			StreamMap streamMap = localCluster.getTopology().getTopology().get(component.getComponentId()).get(componentId);
			GroupingsSet groupingsSet = streamMap.get(streamId);
			if(groupingsSet!=null) {
				for(AbstractGrouping grouping : groupingsSet) {
					List<Integer> taskIds = grouping.chooseTasks(0, values);
					receivingTaskIds.addAll(taskIds);
					component.incTransferred(receivingTaskIds.size()); // for metrics
					HashSet<Integer> remoteTaskIds=new HashSet<Integer>();
					for(Integer taskId : taskIds){
						if(!localCluster.getBolts().containsKey(componentId) || !localCluster.getBolts().get(componentId).containsKey(taskId)){
							remoteTaskIds.add(taskId);
						}
					}
					if(!remoteTaskIds.isEmpty()){
						NetworkTask task = RecycleStation.getInstance()
								.getNetworkTaskRecycler().newObject();
						task.init(tuple, remoteTaskIds, componentId, localCluster.getTopologyId());
						try {
							localCluster.getNode().getRouter().put(task);
						} catch (InterruptedException e) {
							log.error("failed to emit tuple: "+e.toString());
						} 
						
					}
					HashSet<Integer> localTaskIds = new HashSet<Integer>(taskIds);
					
					localTaskIds.removeAll(remoteTaskIds);
					if(!localTaskIds.isEmpty()){
						try {
							NetworkTask task = RecycleStation.getInstance()
									.getNetworkTaskRecycler().newObject();
							
							task.init(tuple, localTaskIds, componentId, localCluster.getTopologyId());
							getQueue(componentId,streamId).put(task);
							localCluster.outputPending(getQueue(componentId,streamId));
						} catch (InterruptedException e) {
							log.error("failed to emit tuple: "+e.toString());
						}
					}
				}
			}
			
		}
		RecycleStation.getInstance().getTupleRecycler(tuple.getFields().getFieldNamesAsString()).crushRecyclable(tuple, 1);
		setEmit();
		return receivingTaskIds;
	}
	
	public synchronized void emitDirect(int taskId, Values values){
		emitDirect(taskId,Constants.DEFAULT_STREAM,values);
	}
	
	// TODO: update this method for network operation - following above example
	public synchronized void emitDirect(int taskId, String streamId, Values values){
		List<Integer> receivingTaskIds = new ArrayList<Integer>();
		Fields fields = component.getOutputFieldsDeclarer().getFieldsDirect(streamId);
		if(fields==null) {
			localCluster.setShouldTerminate("no fields have been declared for ["+
					component.getComponentId()+"] on direct stream ["+streamId+
					"] however it is attempting to emit on that stream");
		}
		if(values.size()!=fields.getFieldNames().length) {
			localCluster.setShouldTerminate("the number of values in ["+values+
					"] does not match the number of fields ["+
					fields.getFieldNamesAsString()+"]");
		}
		Tuple tuple = new Tuple(fields,values);
		tuple.setSourceComponent(component.getComponentId());
		tuple.setSourceTaskId(component.getTaskId());
		tuple.setSourceStreamId(streamId);
		for(String componentId : localCluster.getTopology().getTopology().get(component.getComponentId()).keySet()) {
			//StreamMap toComponent = localCluster.getTopology().topology.get(component.getComponentId()).get(componentId);
			List<Integer> taskIds = new ArrayList<Integer>();
			receivingTaskIds.add(taskId);
//			try {
//				getQueue(componentId,streamId).put(new NetworkTask(tuple,new HashSet<Integer>(taskIds),componentId,localCluster.getTopologyId()));
//				localCluster.outputPending(getQueue(componentId,streamId));
//			} catch (InterruptedException e) {
//				log.error("failed to emit tuple: "+e.toString());
//			}
		}
		setEmit();
	}
	
	@Deprecated
	public synchronized void emitDirect(int taskId, String streamId, Tuple anchorTuple, Values values){
		emitDirect(taskId,Constants.DEFAULT_STREAM,values);
	}
	
	public void resetEmit() {
		emitted=false;
	}
	
	public boolean didEmit() {
		return emitted;
	}
	
	public void setEmit() {
		emitted=true;
	}
	
}
