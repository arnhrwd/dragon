package dragon.topology.base;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.Constants;
import dragon.LocalCluster;
import dragon.grouping.AbstractGrouping;
import dragon.network.Router;
import dragon.topology.DestComponentMap;
import dragon.topology.GroupingsSet;
import dragon.topology.StreamMap;
import dragon.tuple.Fields;
import dragon.tuple.NetworkTask;
import dragon.tuple.RecycleStation;
import dragon.tuple.Recycler;
import dragon.tuple.Tuple;
import dragon.tuple.Values;
import dragon.utils.ComponentTaskBuffer;
import dragon.utils.NetworkTaskBuffer;

/**
 * @author aaron
 *
 */
public class Collector {
	private static final Logger log = LogManager.getLogger(Collector.class);
	
	/**
	 * 
	 */
	private final ComponentTaskBuffer outputQueues;
	
	/**
	 * 
	 */
	private final LocalCluster localCluster;
	
	/**
	 * 
	 */
	private final Component component;
	
	/**
	 * 
	 */
	private final int totalBufferSpace;
	
	/**
	 * 
	 */
	private boolean emitted;
	
	/**
	 * 
	 */
	private final Router router;
	
	/**
	 * 
	 */
	private final HashSet<Integer> doneTaskIds;
	
	/**
	 * 
	 */
	private final HashMap<String,Tuple[]> tuplePool;
	private HashMap<String,Integer> tuplePoolIndex;
	private final NetworkTask[] ntPool;
	private int ntPoolIndex;
	
	
	/**
	 * 
	 * @author aaron
	 *
	 */
	private class TupleBundle {
		public long timestamp;
		public Tuple[] tuples;
		public int size=0;
		public String componentId;
		public String streamId;
		public HashSet<Integer> taskIds;
		public TupleBundle(String componentId,String streamId,HashSet<Integer> taskIds) {
			timestamp=Instant.now().toEpochMilli();
			tuples=new Tuple[bundleSize];
			this.componentId=componentId;
			this.streamId=streamId;
			this.taskIds=taskIds;
		}
		public void add(Tuple tuple) {
			tuples[size++]=tuple;
//			RecycleStation.getInstance()
//			.getTupleRecycler(tuple.getFields()
//					.getFieldNamesAsString()).shareRecyclable(tuple,1);
		}
	}
	
	/**
	 * 
	 */
	private final long linger_ms;

	/**
	 * 
	 */
	private final int bundleSize;
	
	/**
	 * 
	 */
	private HashMap<String,HashMap<String,HashMap<HashSet<Integer>,TupleBundle>>> bundleMap;
	private ArrayList<TupleBundle> bundleList;
	
	/**
	 * @param component
	 * @param localCluster
	 * @param bufSize
	 */
	public Collector(Component component,LocalCluster localCluster,int bufSize) {
		this.component = component;
		this.localCluster=localCluster;
		if(localCluster.getNode()!=null) {
			router=localCluster.getNode().getRouter();
		} else {
			router=null;
		}
		linger_ms=localCluster.getConf().getDragonTupleBundleLingerMS();
		bundleSize=localCluster.getConf().getDragonTupleBundleSize();
		outputQueues=new ComponentTaskBuffer(bufSize);
		bundleMap=new HashMap<>();
		bundleList=new ArrayList<>();
		tuplePool=new HashMap<>();//Tuple[localCluster.getConf().getDragonTupleBundleSize()];
		tuplePoolIndex=new HashMap<>();
		ntPool=new NetworkTask[localCluster.getConf().getDragonTupleBundleSize()];
		ntPoolIndex=0;
		RecycleStation.getInstance().getNetworkTaskRecycler().fillPool(ntPool);
		DestComponentMap destComponentMap = localCluster.getTopology().getDestComponentMap(component.getComponentId());
		int tbs=0;
		if(destComponentMap!=null) {
			for(String destId : destComponentMap.keySet()) {
				bundleMap.put(destId,new HashMap<>());
				for(String streamId : destComponentMap.get(destId).keySet() ) {
					outputQueues.create(destId, streamId);
					bundleMap.get(destId).put(streamId,new HashMap<>());
					tbs+=bufSize;
					Fields fields = component.getOutputFieldsDeclarer().getFields(streamId);
					if(!tuplePool.containsKey(fields.getFieldNamesAsString())) {
						log.debug("creating tuple pool for "+fields.getFieldNamesAsString());
						Tuple[] tuples = new Tuple[localCluster.getConf().getDragonTupleBundleSize()];
						RecycleStation.getInstance().getTupleRecycler(fields.getFieldNamesAsString()).fillPool(tuples);
						tuplePool.put(fields.getFieldNamesAsString(),tuples);
						tuplePoolIndex.put(fields.getFieldNamesAsString(),0);
					}
				}
			}
		}
		totalBufferSpace=tbs;
		doneTaskIds=new HashSet<>();
	}
	
	public void freePools() {
		for(;ntPoolIndex<ntPool.length;ntPoolIndex++) {
			RecycleStation.getInstance().getNetworkTaskRecycler().crushRecyclable(ntPool[ntPoolIndex], 1);
		}
		for(String name : tuplePool.keySet()) {
			for(int i=tuplePoolIndex.get(name);i<tuplePool.get(name).length;i++) {
				RecycleStation.getInstance().getTupleRecycler(name).crushRecyclable(tuplePool.get(name)[i], 1);
			}
		}
	}
	
	/**
	 * @return
	 */
	public int getTotalBufferSpace() {
		return totalBufferSpace;
	}
	
	/**
	 * @param componentId
	 * @param streamId
	 * @return
	 */
	public NetworkTaskBuffer getQueue(String componentId, String streamId){
		return outputQueues.get(componentId).get(streamId);
	}
	
	/**
	 * @return
	 */
	public ComponentTaskBuffer getComponentTaskBuffer() {
		return outputQueues;
	}
	
	/**
	 * @param anchorTuple
	 * @param values
	 * @return
	 */
	@Deprecated
	public synchronized List<Integer> emit(Tuple anchorTuple, Values values) {
		return emit(values);
	}
	
	/**
	 * @param streamId
	 * @param anchorTuple
	 * @param values
	 * @return
	 */
	@Deprecated
	public synchronized List<Integer> emit(String streamId,Tuple anchorTuple, Values values) {
		return emit(streamId,values);
	}
	
	/**
	 * @param values
	 * @return
	 */
	public synchronized List<Integer> emit(Values values){
		return emit(Constants.DEFAULT_STREAM,values);
	}
	
	/**
	 * 
	 * @param tb
	 */
	private void transmitBundle(TupleBundle tb) {
		transmit(tb.tuples,tb.taskIds,tb.componentId,tb.streamId);
		tb.timestamp=0; // nullify this in the list
	}
	
	/**
	 * 
	 */
	public void expireTupleBundles() {
		if(bundleList.size()==0)return;
		long now = Instant.now().toEpochMilli();
		boolean hit=true;
		while(hit && bundleList.size()>0) {
			hit=false;
			TupleBundle tb = bundleList.remove(0);
			while(tb.timestamp==0 && bundleList.size()>0) tb=bundleList.remove(0);
			if(tb.timestamp==0) return;
			if(now-tb.timestamp > linger_ms) {
				transmitBundle(tb);
				bundleMap.get(tb.componentId).get(tb.streamId).remove(tb.taskIds);
				hit=true;
			} 
		}
		
	}
	
	/**
	 * 
	 */
	public void expireAllTupleBundles() {
		while(bundleList.size()>0) {
			TupleBundle tb = bundleList.remove(0);
			if(tb.timestamp > 0) {
				transmitBundle(tb);
				bundleMap.get(tb.componentId).get(tb.streamId).remove(tb.taskIds);
			} 
		}
		
	}
	
	/**
	 * 
	 * @param tuple
	 * @param taskIds
	 * @param componentId
	 * @param streamId
	 */
	private void transmit(Tuple tuple,
			List<Integer> taskIds,
			String componentId,
			String streamId) {
		HashSet<Integer> taskIdSet=new HashSet<Integer>(taskIds);
		if(!bundleMap.get(componentId).get(streamId).containsKey(taskIdSet)) {
			TupleBundle tb=new TupleBundle(componentId,streamId,taskIdSet);
			bundleMap.get(componentId).get(streamId).put(taskIdSet,tb);
			bundleList.add(tb);
		}
		TupleBundle tb = bundleMap.get(componentId).get(streamId).get(taskIdSet);
		tb.add(tuple);
		if(tb.size==tb.tuples.length)  {
			transmitBundle(tb);
			bundleMap.get(componentId).get(streamId).remove(taskIdSet);
		} 
	}
	
	/**
	 * @param tuples
	 * @param taskIds
	 * @param componentId
	 * @param streamId
	 */
	private void transmit(Tuple[] tuples,
			HashSet<Integer> taskIds,
			String componentId,
			String streamId) {
		
		HashSet<Integer> remoteTaskIds=new HashSet<Integer>();
		for(Integer taskId : taskIds){
			if(!localCluster.getBolts().containsKey(componentId) || !localCluster.getBolts().get(componentId).containsKey(taskId)){
				remoteTaskIds.add(taskId);
			}
		}
		if(!remoteTaskIds.isEmpty()){
			NetworkTask task = ntPool[ntPoolIndex++];
			if(ntPoolIndex==ntPool.length) {
				RecycleStation.getInstance().getNetworkTaskRecycler().fillPool(ntPool);
				ntPoolIndex=0;
			}
			task.init(tuples, remoteTaskIds, componentId, localCluster.getTopologyId());
			try {
				router.put(task);
			} catch (InterruptedException e) {
				log.info("interrupted");
				return;
			}
			
		}
		HashSet<Integer> localTaskIds = new HashSet<Integer>(taskIds);
		
		localTaskIds.removeAll(remoteTaskIds);
		/*
		 * Bulk share these tuples:
		 * We have 1 ref, which we wont need anymore
		 * We are are sharing to local task ids + one network task
		 */
		RecycleStation.getInstance()
			.getTupleRecycler(tuples[0].getFields()
			.getFieldNamesAsString())
			.shareRecyclables(tuples,localTaskIds.size()-1);
		if(!localTaskIds.isEmpty()){
			/*
			 * First try to directly send the tuple to the input queue(s).
			 *
			 * To maintain order this can only be done if the output queue
			 * is empty. There is no race condition with the output scheduler
			 * since it does not poll the queue until it is done working on the
			 * current head of the queue, if it exists.
			 */
			final NetworkTaskBuffer queue=getQueue(componentId,streamId);
			final boolean empty=queue.isEmpty();
			final HashMap<Integer,Bolt> destComp = localCluster.getBolts().get(componentId);
			if(empty) {
				doneTaskIds.clear();
				for(Integer taskId:localTaskIds) {
					if(destComp.get(taskId).getInputCollector().getQueue().offer(tuples))
						doneTaskIds.add(taskId);
				}
				localTaskIds.removeAll(doneTaskIds);
			}
			
			/*
			 * What we couldn't transmit ourselves, we leave to
			 * the output scheduler. 
			 */
			if(!localTaskIds.isEmpty()) {
				NetworkTask task = ntPool[ntPoolIndex++];
				if(ntPoolIndex==ntPool.length) {
					RecycleStation.getInstance().getNetworkTaskRecycler().fillPool(ntPool);
					ntPoolIndex=0;
				}
				task.init(tuples, localTaskIds, componentId, localCluster.getTopologyId());
				try {
					getQueue(componentId,streamId).put(task);
					if(queue.size()==1)localCluster.outputPending(queue);
				} catch (InterruptedException e) {
					log.info("interrupted");
					return;
				}
				
			}
		}
	}
	
	/**
	 * @param streamId
	 * @param values
	 * @return
	 */
	public synchronized List<Integer> emit(String streamId,Values values) {
		List<Integer> receivingTaskIds = new ArrayList<Integer>();
		if(component.isClosed()) {
			log.error("spontaneous tuple emission after close, topology may not terminate properly ["+component.getComponentId()+":"+component.getTaskId()+"]");
			return receivingTaskIds;
		}
		Fields fields = component.getOutputFieldsDeclarer().getFields(streamId);
		if(fields==null) {
			throw new DragonEmitRuntimeException("no fields have been declared for ["+
					component.getComponentId()+"] on stream ["+streamId+
					"] however it is attempting to emit on that stream");
		}
		if(values.size()!=fields.getFieldNames().length) {
			throw new DragonEmitRuntimeException("the number of values in ["+values+
					"] does not match the number of fields ["+
					fields.getFieldNamesAsString()+"]");
		}
		final String fieldsName = fields.getFieldNamesAsString();
		final int index=tuplePoolIndex.get(fieldsName);
		Tuple tuple = tuplePool.get(fieldsName)[index];
		if(index+1==tuplePool.get(fieldsName).length) {
			RecycleStation.getInstance()
			.getTupleRecycler(fieldsName)
			.fillPool(tuplePool.get(fieldsName));
			tuplePoolIndex.put(fieldsName,0);
		} else {
			tuplePoolIndex.put(fieldsName,index+1);
		}
		
		
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
					transmit(tuple,
							taskIds,
							componentId,
							streamId); 
				}
			}
			
		}
		setEmit();
		return receivingTaskIds;
	}
	
	/**
	 * @param taskId
	 * @param values
	 */
	public synchronized void emitDirect(int taskId, Values values){
		emitDirect(taskId,Constants.DEFAULT_STREAM,values);
	}
	
	// TODO: update this method for network operation - following above example
	/**
	 * @param taskId
	 * @param streamId
	 * @param values
	 */
	public synchronized void emitDirect(int taskId, String streamId, Values values){
		List<Integer> receivingTaskIds = new ArrayList<Integer>();
		Fields fields = component.getOutputFieldsDeclarer().getFieldsDirect(streamId);
//		if(fields==null) {
//			localCluster.setShouldTerminate("no fields have been declared for ["+
//					component.getComponentId()+"] on direct stream ["+streamId+
//					"] however it is attempting to emit on that stream");
//		}
//		if(values.size()!=fields.getFieldNames().length) {
//			localCluster.setShouldTerminate("the number of values in ["+values+
//					"] does not match the number of fields ["+
//					fields.getFieldNamesAsString()+"]");
//		}
		Tuple tuple = new Tuple(fields,values);
		tuple.setSourceComponent(component.getComponentId());
		tuple.setSourceTaskId(component.getTaskId());
		tuple.setSourceStreamId(streamId);
		for(String componentId : localCluster.getTopology().getTopology().get(component.getComponentId()).keySet()) {
			//StreamMap toComponent = localCluster.getTopology().topology.get(component.getComponentId()).get(componentId);
			List<Integer> taskIds = new ArrayList<Integer>();
			receivingTaskIds.add(taskId);
		}
		setEmit();
	}
	
	/**
	 * @param taskId
	 * @param streamId
	 * @param anchorTuple
	 * @param values
	 */
	@Deprecated
	public synchronized void emitDirect(int taskId, String streamId, Tuple anchorTuple, Values values){
		emitDirect(taskId,Constants.DEFAULT_STREAM,values);
	}
	
	/**
	 * 
	 */
	public void resetEmit() {
		emitted=false;
	}
	
	/**
	 * @return
	 */
	public boolean didEmit() {
		return emitted;
	}
	
	/**
	 * 
	 */
	public void setEmit() {
		emitted=true;
	}

	/**
	 * 
	 */
	public void emitTerminateTuple() {
		if(localCluster.getTopology().getTopology().get(component.getComponentId())==null) return;
		for(String componentId : localCluster.getTopology().getTopology().get(component.getComponentId()).keySet()) {
			StreamMap streamMap = localCluster.getTopology().getTopology().get(component.getComponentId()).get(componentId);
			for(String streamId : streamMap.keySet()) {
				// in this special case, we also use the grouping defined on the system stream, since 
				// that is a single "all group".
				GroupingsSet groupingsSet = streamMap.get(Constants.SYSTEM_STREAM_ID);
				Tuple tuple = RecycleStation.getInstance()
						.getTupleRecycler(new Fields(Constants.SYSTEM_TUPLE_FIELDS).getFieldNamesAsString())
						.newObject();
				tuple.setSourceComponent(component.getComponentId());
				tuple.setSourceStreamId(streamId);
				tuple.setSourceTaskId(component.getTaskId());
				tuple.setType(Tuple.Type.TERMINATE);
				for(AbstractGrouping grouping : groupingsSet) {
					List<Integer> taskIds = grouping.chooseTasks(0, null);
					transmit(tuple,
							taskIds,
							componentId,
							streamId); 
				}
				RecycleStation.getInstance().getTupleRecycler(tuple.getFields().getFieldNamesAsString()).crushRecyclable(tuple, 1);
			}
		}
	}
	
	/**
	 * 
	 * @return
	 */
	public long getLinger_ms() {
		return linger_ms;
	}

	/**
	 * 
	 * @return
	 */
	public int getBundleSize() {
		return bundleSize;
	}
	
}
