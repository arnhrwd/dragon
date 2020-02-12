package dragon.network;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.Config;
import dragon.LocalCluster;
import dragon.network.comms.DragonCommsException;
import dragon.network.comms.IComms;
import dragon.topology.DragonTopology;
import dragon.tuple.NetworkTask;
import dragon.tuple.RecycleStation;
import dragon.utils.NetworkTaskBuffer;

/**
 * The Router is responsible for taking NetworkTasks from the LocalCluster and duplicating
 * them as required to be sent to possibly multiple destination machines. A single copy
 * of the NetworkTask is sent to each machine that requires it. When received from another
 * machine, the Router is responsible for making the NetworkTask available to the LocalCluster
 * which in turn copies the enclosed tuple to the relevant bolts receiving the tuple.
 * 
 * @author aaron
 *
 */
public class Router {
	private final static Logger log = LogManager.getLogger(Router.class);

	/**
	 * 
	 */
	//private final Node node;
	
	/**
	 * 
	 */
	private final IComms comms;
	
	/**
	 * 
	 */
	private final HashMap<String, LocalCluster> localClusters;

	/**
	 * 
	 */
	private final ArrayList<Thread> outgoingThreads;

	/**
	 * 
	 */
	private final ArrayList<Thread> incomingThreads;

	/**
	 * 
	 */
	private final TopologyQueueMap inputQueues;

	/**
	 * 
	 */
	private final TopologyQueueMap outputQueues;

	/**
	 * 
	 */
	private boolean shouldTerminate=false;

	/**
	 * 
	 */
	private final Config conf;

	/**
	 * 
	 */
	private final LinkedBlockingQueue<NetworkTaskBuffer> outputsPending;

	/**
	 * @param node
	 * @param comms
	 * @param localClusters
	 */
	public Router(Config conf,IComms comms,HashMap<String, LocalCluster> localClusters) {
		//this.node=node;
		this.comms=comms;
		this.localClusters=localClusters;
		this.conf=conf;
		inputQueues = new TopologyQueueMap((Integer)conf.getDragonRouterInputBufferSize());
		outputQueues = new TopologyQueueMap((Integer)conf.getDragonRouterOutputBufferSize());
		outgoingThreads = new ArrayList<>();
		incomingThreads = new ArrayList<>();
		
		// A circular blocking queue is not so applicable here because the input and output queues
		// change in response to topologies being started and stopped. TODO: write a linked 
		// blocking queue that reuses reference objects from a pool, rather than new'ing and
		// dereferencing them.
		outputsPending=new LinkedBlockingQueue<>();
		
		// Startup the thread
		runExecutors();
	}
	
	/**
	 * terminate the router
	 */
	public void terminate() {
		for(Thread thread : outgoingThreads) thread.interrupt();
		for(Thread thread : incomingThreads) thread.interrupt();
		if(outputsPending.size()>0) log.error("there are still outputs pending");
		if(!inputQueues.isEmpty()||!outputQueues.isEmpty()) log.error("some io queues are not empty");
	}
	
	/**
	 * 
	 */
	private void runExecutors() {
		for(int i=0;i<(Integer)conf.getDragonRouterOutputThreads();i++) {
			outgoingThreads.add(new Thread() {
				@Override
				public void run() {
					log.info("starting up");
					while(!shouldTerminate) {
						try {
							NetworkTaskBuffer buffer = outputsPending.take();
							HashMap<NodeDescriptor,HashSet<Integer>> destinations = new HashMap<>();
							buffer.bufferLock.lock();
							try {
								NetworkTask task = buffer.poll();
								if(task!=null){
									HashSet<Integer> taskIds=task.getTaskIds();
									HashMap<Integer,NodeDescriptor> taskMap = localClusters
											.get(task.getTopologyId())
											.getTopology()
											.getEmbedding()
											.get(task.getComponentId());
									destinations.clear();
									for(Integer taskId : taskIds) {
										NodeDescriptor desc = taskMap.get(taskId);
										if(!destinations.containsKey(desc)) {
											destinations.put(desc,new HashSet<Integer>());
										}
										HashSet<Integer> tasks = destinations.get(desc);
										tasks.add(taskId);
									}
									for(NodeDescriptor desc : destinations.keySet()) {
										NetworkTask nt = RecycleStation.getInstance()
												.getNetworkTaskRecycler().newObject();
										nt.init(task.getTuple(),
												destinations.get(desc),
												task.getComponentId(),
												task.getTopologyId());
										//log.debug("seding to "+desc+" "+nt);
										try {
											comms.sendNetworkTask(desc, nt);
										} catch (DragonCommsException e) {
											log.error("failed to send network task to ["+desc+"]");
										}
										RecycleStation.getInstance().getNetworkTaskRecycler().crushRecyclable(nt, 1);
									}
									RecycleStation.getInstance().getNetworkTaskRecycler().crushRecyclable(task, 1);
								}
							} finally {
								buffer.bufferLock.unlock();
							}
						} catch (InterruptedException e) {
							log.info("interrupted while taking from queue");
						}
					}
					log.info("starting up");
				}
			});
			outgoingThreads.get(i).setName("router outgoing "+i);
			outgoingThreads.get(i).start();
		}
		for(int i=0;i<(Integer)conf.getDragonRouterInputThreads();i++) {
			incomingThreads.add(new Thread() {
				@Override
				public void run() {
					log.info("starting up");
					while(!shouldTerminate) {
						NetworkTask task;
						try {
							task = comms.receiveNetworkTask();
						} catch (InterruptedException e1) {
							log.info("interrupted");
							break;
						}
						try {
							if(localClusters.containsKey(task.getTopologyId())) {
								RecycleStation.getInstance().getNetworkTaskRecycler().shareRecyclable(task, 1);
								inputQueues.put(task);
								localClusters.get(task.getTopologyId()).outputPending(inputQueues.getBuffer(task));
								RecycleStation.getInstance().getNetworkTaskRecycler().crushRecyclable(task, 1);
							} else {
								log.error("received a network task for a non-existant topology ["+task.getTopologyId()+"]");
							}
						} catch (InterruptedException e) {
							log.info("interrupted");
							break;
						}
					}
					log.info("shutting down");
				}
			});
			incomingThreads.get(i).setName("router incoming "+i);
			incomingThreads.get(i).start();
		}
	}
	
	/**
	 * @param task
	 * @return
	 */
	public boolean offer(NetworkTask task) {
		RecycleStation.getInstance().getNetworkTaskRecycler().shareRecyclable(task, 1);
		boolean ret = outputQueues.getBuffer(task).offer(task);
		if(ret){
			try {
				outputsPending.put(outputQueues.getBuffer(task));
			} catch (InterruptedException e) {
				log.error("interrupted while waiting to put on outputs pending");
			}
			
		}
		RecycleStation.getInstance().getNetworkTaskRecycler().crushRecyclable(task, 1);
		return ret;
	}
	
	
	/**
	 * @param task
	 * @throws InterruptedException
	 */
	public void put(NetworkTask task) throws InterruptedException {
		
		//log.debug("putting on queue "+task.getTopologyId()+","+task.getTuple().getSourceStreamId());
		RecycleStation.getInstance().getNetworkTaskRecycler().shareRecyclable(task, 1);
		outputQueues.getBuffer(task).put(task);
		outputsPending.put(outputQueues.getBuffer(task));
		RecycleStation.getInstance().getNetworkTaskRecycler().crushRecyclable(task, 1);
	}

	/**
	 * @param topologyName
	 * @param topology
	 */
	public void submitTopology(String topologyName, DragonTopology topology) {
		for(NodeDescriptor desc : topology.getReverseEmbedding().keySet()) {
			if(!desc.equals(comms.getMyNodeDesc())) {
				for(String componentId : topology.getReverseEmbedding().get(desc).keySet()) {
					if(!topology.getBoltMap().containsKey(componentId))continue;
					for(String listened : topology.getBoltMap().get(componentId).groupings.keySet()) {
						for(String streamId : topology.getBoltMap().get(componentId).groupings.get(listened).keySet()) {
							log.debug("preparing output queue ["+topologyName+","+componentId+","+streamId+"]");
							outputQueues.prepare(topologyName,componentId,streamId);
						}
					}
				}
			} else {
				for(String componentId : topology.getReverseEmbedding().get(desc).keySet()) {
					if(!topology.getBoltMap().containsKey(componentId))continue;
					for(String listened : topology.getBoltMap().get(componentId).groupings.keySet()) {
						for(String streamId : topology.getBoltMap().get(componentId).groupings.get(listened).keySet()) {
							log.debug("preparing input queue ["+topologyName+","+componentId+","+streamId+"]");
							inputQueues.prepare(topologyName,componentId,streamId);
						}
					}
				}
			}
		}
		
	}
	
	/**
	 * @param topologyName
	 * @param topology
	 */
	public void terminateTopology(String topologyName, DragonTopology topology) {
		for(NodeDescriptor desc : topology.getReverseEmbedding().keySet()) {
			if(!desc.equals(comms.getMyNodeDesc())) {
				for(String componentId : topology.getReverseEmbedding().get(desc).keySet()) {
					if(!topology.getBoltMap().containsKey(componentId))continue;
					for(String listened : topology.getBoltMap().get(componentId).groupings.keySet()) {
						for(String streamId : topology.getBoltMap().get(componentId).groupings.get(listened).keySet()) {
							log.debug("dropping output queue ["+topologyName+","+componentId+","+streamId+"]");
							outputQueues.drop(topologyName,componentId,streamId);
						}
					}
				}
			} else {
				for(String componentId : topology.getReverseEmbedding().get(desc).keySet()) {
					if(!topology.getBoltMap().containsKey(componentId))continue;
					for(String listened : topology.getBoltMap().get(componentId).groupings.keySet()) {
						for(String streamId : topology.getBoltMap().get(componentId).groupings.get(listened).keySet()) {
							log.debug("dropping input queue ["+topologyName+","+componentId+","+streamId+"]");
							inputQueues.drop(topologyName,componentId,streamId);
						}
					}
				}
			}
		}
	}
	
}
