package dragon.topology.base;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Constants;
import dragon.LocalCluster;
import dragon.NetworkTask;
import dragon.grouping.CustomStreamGrouping;
import dragon.tuple.Fields;
import dragon.tuple.Tuple;
import dragon.tuple.Values;
import dragon.utils.CircularBuffer;


public class Collector {
	private Log log = LogFactory.getLog(Collector.class);
	private CircularBuffer<NetworkTask> outputQueue;
	private LocalCluster localCluster;
	private Component component;
	
	private boolean emitted;
	
	public Collector(Component component,LocalCluster localCluster,int bufSize) {
		this.component = component;
		this.localCluster=localCluster;
		outputQueue=new CircularBuffer<NetworkTask>(bufSize);
	}
	
	public CircularBuffer<NetworkTask> getQueue(){
		return outputQueue;
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
		Tuple tuple = new Tuple(fields,values);
		tuple.setSourceComponent(component.getComponentId());
		tuple.setSourceTaskId(component.getTaskId());
		tuple.setSourceStreamId(streamId);
		for(String componentId : localCluster.getTopology().topology.
				get(component.getComponentId()).keySet()) {
			HashMap<String,HashSet<CustomStreamGrouping>> toComponent = 
					localCluster.getTopology().topology.get(component.getComponentId()).get(componentId);
			HashSet<CustomStreamGrouping> stream = toComponent.get(streamId);
			if(stream!=null) {
				for(CustomStreamGrouping grouping : stream) {
					List<Integer> taskIds = grouping.chooseTasks(0, values);
					receivingTaskIds.addAll(taskIds);
					try {
						outputQueue.put(new NetworkTask(tuple,new HashSet<Integer>(taskIds),componentId));
						localCluster.outputPending(this.outputQueue);
					} catch (InterruptedException e) {
						log.error("failed to emit tuple: "+e.toString());
					}
				}
			}
			
		}
		setEmit();
		return receivingTaskIds;
	}
	
	public synchronized void emitDirect(int taskId, Values values){
		emitDirect(taskId,Constants.DEFAULT_STREAM,values);
	}
	
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
		for(String componentId : localCluster.getTopology().topology.
				get(component.getComponentId()).keySet()) {
			HashMap<String,HashSet<CustomStreamGrouping>> toComponent = 
					localCluster.getTopology().topology.get(component.getComponentId()).get(componentId);
			List<Integer> taskIds = new ArrayList<Integer>();
			receivingTaskIds.add(taskId);
			try {
				outputQueue.put(new NetworkTask(tuple,new HashSet<Integer>(taskIds),componentId));
				localCluster.outputPending(this.outputQueue);
			} catch (InterruptedException e) {
				log.error("failed to emit tuple: "+e.toString());
			}
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
