package dragon.network;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.NetworkTask;
import dragon.topology.DragonTopology;
import dragon.utils.NetworkTaskBuffer;

public class Router {
	private static Log log = LogFactory.getLog(Router.class);
	private Node node;
	private ExecutorService outgoingExecutorService;
	private ExecutorService incommingExecutorService;
	private TopologyQueueMap inputQueues;
	private TopologyQueueMap outputQueues;
	private boolean shouldTerminate=false;
	private Config conf;
	private LinkedBlockingQueue<NetworkTaskBuffer> outputsPending;
	public Router(Node node, Config conf) {
		this.node=node;
		this.conf=conf;
		inputQueues = new TopologyQueueMap((Integer)conf.getDragonRouterInputBufferSize());
		outputQueues = new TopologyQueueMap((Integer)conf.getDragonRouterOutputBufferSize());
		outgoingExecutorService = Executors.newFixedThreadPool((Integer)conf.getDragonRouterOutputThreads());
		incommingExecutorService = Executors.newFixedThreadPool((Integer)conf.getDragonRouterInputThreads());
		outputsPending=new LinkedBlockingQueue<NetworkTaskBuffer>();
		runExecutors();
	}
	
	private void runExecutors() {
		for(int i=0;i<(Integer)conf.getDragonRouterOutputThreads();i++) {
			outgoingExecutorService.execute(new Runnable() {
				public void run() {
					while(!shouldTerminate) {
						try {
							NetworkTaskBuffer buffer = outputsPending.take();
							synchronized(buffer.lock){
								NetworkTask task = buffer.poll();
								if(task!=null){
									HashSet<Integer> taskIds=task.getTaskIds();
									HashMap<Integer,NodeDescriptor> taskMap = node
											.getLocalClusters()
											.get(task.getTopologyId())
											.getTopology()
											.getEmbedding()
											.get(task.getComponentId());
									HashMap<NodeDescriptor,HashSet<Integer>> destinations = 
											new HashMap<NodeDescriptor,HashSet<Integer>>();
									for(Integer taskId : taskIds) {
										NodeDescriptor desc = taskMap.get(taskId);
										if(!destinations.containsKey(desc)) {
											destinations.put(desc,new HashSet<Integer>());
										}
										HashSet<Integer> tasks = destinations.get(desc);
										tasks.add(taskId);
									}
									for(NodeDescriptor desc : destinations.keySet()) {
										NetworkTask nt = new NetworkTask(task.getTuple(),
												destinations.get(desc),
												task.getComponentId(),
												task.getTopologyId());
										//log.debug("seding to "+desc+" "+nt);
										node.getComms().sendNetworkTask(desc, nt);
									}
								}
							}
						} catch (InterruptedException e) {
							log.debug("interrupted while taking from queue");
						}
					}
				}
			});
		}
		for(int i=0;i<(Integer)conf.getDragonRouterInputThreads();i++) {
			incommingExecutorService.execute(new Runnable() {
				public void run() {
					while(!shouldTerminate) {
						NetworkTask task = node.getComms().receiveNetworkTask();
						try {
							if(node.getLocalClusters().containsKey(task.getTopologyId())) {
								inputQueues.put(task);
								node.getLocalClusters().get(task.getTopologyId()).outputPending(inputQueues.getBuffer(task));
							} else {
								log.error("received a network task for a non-existant topology ["+task.getTopologyId()+"]");
							}
						} catch (InterruptedException e) {
							log.info("interrupted");
							break;
						}
					}
				}
			});
		}
	}
	
	public boolean offer(NetworkTask task) {
		boolean ret = outputQueues.getBuffer(task).offer(task);
		if(ret){
			try {
				outputsPending.put(outputQueues.getBuffer(task));
			} catch (InterruptedException e) {
				log.error("interrupted while waiting to put on outputs pending");
			}
		}
		return ret;
	}
	
	public void put(NetworkTask task) throws InterruptedException {
		//log.debug("putting on queue "+task.getTopologyId()+","+task.getTuple().getSourceStreamId());
		outputQueues.getBuffer(task).put(task);
		outputsPending.put(outputQueues.getBuffer(task));
	}

	public void submitTopology(String topologyName, DragonTopology topology) {
		for(NodeDescriptor desc : topology.getReverseEmbedding().keySet()) {
			if(!desc.equals(node.getComms().getMyNodeDescriptor())) {
				for(String componentId : topology.getReverseEmbedding().get(desc).keySet()) {
					if(!topology.getBoltMap().containsKey(componentId))continue;
					for(String listened : topology.getBoltMap().get(componentId).groupings.keySet()) {
						for(String streamId : topology.getBoltMap().get(componentId).groupings.get(listened).keySet()) {
							log.debug("preparing output queue ["+topologyName+","+streamId+"]");
							outputQueues.prepare(topologyName,streamId);
						}
					}
				}
			} else {
				for(String componentId : topology.getReverseEmbedding().get(desc).keySet()) {
					if(!topology.getBoltMap().containsKey(componentId))continue;
					for(String listened : topology.getBoltMap().get(componentId).groupings.keySet()) {
						for(String streamId : topology.getBoltMap().get(componentId).groupings.get(listened).keySet()) {
							log.debug("preparing input queue ["+topologyName+","+streamId+"]");
							inputQueues.prepare(topologyName,streamId);
						}
					}
				}
			}
		}
		
	}
	
	public void terminateTopology(String topologyName, DragonTopology topology) {
		for(NodeDescriptor desc : topology.getReverseEmbedding().keySet()) {
			if(!desc.equals(node.getComms().getMyNodeDescriptor())) {
				for(String componentId : topology.getReverseEmbedding().get(desc).keySet()) {
					if(!topology.getBoltMap().containsKey(componentId))continue;
					for(String listened : topology.getBoltMap().get(componentId).groupings.keySet()) {
						for(String streamId : topology.getBoltMap().get(componentId).groupings.get(listened).keySet()) {
							log.debug("dropping output queue ["+topologyName+","+streamId+"]");
							outputQueues.drop(topologyName,streamId);
						}
					}
				}
			} else {
				for(String componentId : topology.getReverseEmbedding().get(desc).keySet()) {
					if(!topology.getBoltMap().containsKey(componentId))continue;
					for(String listened : topology.getBoltMap().get(componentId).groupings.keySet()) {
						for(String streamId : topology.getBoltMap().get(componentId).groupings.get(listened).keySet()) {
							log.debug("dropping input queue ["+topologyName+","+streamId+"]");
							inputQueues.drop(topologyName,streamId);
						}
					}
				}
			}
		}
	}
	
}
