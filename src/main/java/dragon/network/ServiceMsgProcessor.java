package dragon.network;

import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.Constants;
import dragon.DragonInvalidStateException;
import dragon.DragonRequiresClonableException;
import dragon.metrics.ComponentMetricMap;
import dragon.network.Node.NodeState;
import dragon.network.comms.DragonCommsException;
import dragon.network.comms.IComms;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.allocpart.AllocPartErrorSMsg;
import dragon.network.messages.service.allocpart.AllocPartSMsg;
import dragon.network.messages.service.allocpart.PartAllocedSMsg;
import dragon.network.messages.service.dealloc.DeallocPartErrorSMsg;
import dragon.network.messages.service.dealloc.DeallocPartSMsg;
import dragon.network.messages.service.dealloc.PartDeallocedSMsg;
import dragon.network.messages.service.getmetrics.GetMetricsErrorSMsg;
import dragon.network.messages.service.getmetrics.GetMetricsSMsg;
import dragon.network.messages.service.getmetrics.MetricsSMsg;
import dragon.network.messages.service.getnodecontext.GetNodeContextSMsg;
import dragon.network.messages.service.getnodecontext.NodeContextSMsg;
import dragon.network.messages.service.getstatus.GetStatusErrorSMsg;
import dragon.network.messages.service.getstatus.GetStatusSMsg;
import dragon.network.messages.service.getstatus.StatusSMsg;
import dragon.network.messages.service.halttopo.HaltTopoErrorSMsg;
import dragon.network.messages.service.halttopo.HaltTopoSMsg;
import dragon.network.messages.service.halttopo.TopoHaltedSMsg;
import dragon.network.messages.service.listtopo.ListToposErrorSMsg;
import dragon.network.messages.service.listtopo.ListToposSMsg;
import dragon.network.messages.service.listtopo.TopoListSMsg;
import dragon.network.messages.service.progress.ProgressSMsg;
import dragon.network.messages.service.resumetopo.ResumeTopoErrorSMsg;
import dragon.network.messages.service.resumetopo.ResumeTopoSMsg;
import dragon.network.messages.service.resumetopo.TopoResumedMsg;
import dragon.network.messages.service.runtopo.RunTopoErrorSMsg;
import dragon.network.messages.service.runtopo.RunTopoSMsg;
import dragon.network.messages.service.runtopo.TopoRunningSMsg;
import dragon.network.messages.service.termtopo.TermTopoErrorSMsg;
import dragon.network.messages.service.termtopo.TermTopoSMsg;
import dragon.network.messages.service.termtopo.TopoTermdSMsg;
import dragon.network.messages.service.uploadjar.UploadJarFailedSMsg;
import dragon.network.messages.service.uploadjar.UploadJarSMsg;
import dragon.network.messages.service.uploadjar.UploadJarSuccessSMsg;
import dragon.network.operations.AllocPartGroupOp;
import dragon.network.operations.DeallocPartGroupOp;
import dragon.network.operations.DragonInvalidContext;
import dragon.network.operations.GetStatusGroupOp;
import dragon.network.operations.HaltTopoGroupOp;
import dragon.network.operations.ListToposGroupOp;
import dragon.network.operations.Ops;
import dragon.network.operations.PrepTopoGroupOp;
import dragon.network.operations.RemoveTopoGroupOp;
import dragon.network.operations.ResumeTopoGroupOp;
import dragon.network.operations.PrepJarGroupOp;
import dragon.network.operations.StartTopoGroupOp;
import dragon.network.operations.TermTopoGroupOp;
import dragon.topology.DragonTopology;

/**
 * Process service messages that come from a client. Processing may require
 * issuing a sequence of group operations. This object will read service
 * messages one at time from the Comms layer.
 * 
 * @author aaron
 *
 */
public class ServiceMsgProcessor extends Thread {
	private final static Logger log = LogManager.getLogger(ServiceMsgProcessor.class);
	
	/**
	 * Cached node reference.
	 */
	private final Node node;
	
	/**
	 * Cached comms reference.
	 */
	private final IComms comms;

	/**
	 * @param node
	 */
	public ServiceMsgProcessor() {
		this.node = Node.inst();
		this.comms = node.getComms();
		setName("service processor");
		start();
	}
	
	/**
	 * Utility function to send a message to the client.
	 * @param msg
	 * @param dest
	 * @return true if it was sent, false otherwise
	 */
	private boolean client(ServiceMessage msg, ServiceMessage dest) {
		try {
			comms.sendServiceMsg(msg, dest);
		} catch (DragonCommsException e) {
			log.fatal("can't communicate with client: " + e.getMessage());
			return false;
		}
		return true;
	}
	
	/**
	 * Utility function to report progress back to client.
	 * @param msg
	 * @param dest
	 * @return true if message was sent, false otherwise
	 */
	private boolean progress(String msg, ServiceMessage dest) {
		return client(new ProgressSMsg(msg),dest);
	}

	/**
	 * Upload a JAR file to the daemon. JAR files that contain the topology must be
	 * uploaded prior to the run topology commmand, on the same daemon that the run
	 * topology command will be given to.
	 * 
	 * @param msg contains the name of the topology and the JAR byte array
	 */
	private void processUploadJar(UploadJarSMsg msg) {
		if (node.getLocalClusters().containsKey(msg.topologyId)) {
			client(new UploadJarFailedSMsg(msg.topologyId, "topology exists"), msg);
		} else {
			log.info("storing topology [" + msg.topologyId + "]");
			if (!node.storeJarFile(msg.topologyId, msg.topologyJar)) {
				client(new UploadJarFailedSMsg(msg.topologyId, "could not store the topology jar"), msg);
				return;
			}
			if (!node.loadJarFile(msg.topologyId)) {
				client(new UploadJarFailedSMsg(msg.topologyId, "could not load the topology jar"), msg);
				return;
			}
			client(new UploadJarSuccessSMsg(msg.topologyId), msg);
		}
	}

	/**
	 * Run the topology. Running a topology involves the sequence of group
	 * operations:
	 * <ol>
	 * <li>send the jar file to all daemons</li>
	 * <li>allocate the local cluster on all daemons</li>
	 * <li>start the local cluster on all daemons</li>
	 * </ol>
	 * 
	 * @param msg contains the name of the topology and the topology itself
	 */
	private void processRunTopology(RunTopoSMsg msg) {
		if (node.getLocalClusters().containsKey(msg.topologyId)) {
			client(new RunTopoErrorSMsg(msg.topologyId, "topology exists"),msg);
		} else {
			final DragonTopology topo = msg.dragonTopology;
			byte[] jarfile =  node.readJarFile(msg.topologyId);
			if(jarfile==null){
				client(new RunTopoErrorSMsg(msg.topologyId, "could not read the jar file; please upload jar first"),msg);
				return;
			}
			msg.dragonTopology.getReverseEmbedding().keySet().forEach((desc)->{
				if(!node.getNodeProcessor().getContext().containsKey(desc.toString())) {
					client(new RunTopoErrorSMsg(msg.topologyId, "invalid topology context: ["+desc+"] does not exist"),msg);
					return;
				}
			});
			try {
				Ops.inst().newPrepJarGroupOp(msg.topologyId, jarfile, topo, (op) -> {
					try {
						Ops.inst().newPreTopoGroupOp(msg, topo, (op2) -> {
							try {
								Ops.inst().newStartTopologyGroupOp(msg.topologyId, (op3) -> {
									client(new TopoRunningSMsg(msg.topologyId),msg);
								}, (op3, error) -> {
									client(new RunTopoErrorSMsg(msg.topologyId, error),msg);
									node.topologyFault(msg.topologyId,msg.dragonTopology);
								}).onRunning((op3) -> {
									try {
										node.startTopology(msg.topologyId);
									} catch (DragonTopologyException | DragonInvalidStateException e) {
										((StartTopoGroupOp)op3).fail(e.getMessage());
									}
									((StartTopoGroupOp) op3).receiveSuccess(comms.getMyNodeDesc());
									progress("starting topology on each daemon",msg);
								}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(),
										TimeUnit.MILLISECONDS, (op4)->{
									op4.fail("timed out starting the topology");
								});
							} catch (DragonInvalidContext e) {
								client(new RunTopoErrorSMsg(msg.topologyId, e.getMessage()),msg);
							}
						}, (op2, error) -> {
							client(new RunTopoErrorSMsg(msg.topologyId, error),msg);
						}).onRunning((op2) -> {
							try {
								node.prepareTopology(msg.topologyId, msg.conf, topo, false);
								((PrepTopoGroupOp) op2).receiveSuccess(comms.getMyNodeDesc());
							} catch (DragonRequiresClonableException | DragonTopologyException e) {
								((PrepTopoGroupOp) op2).receiveError(comms.getMyNodeDesc(), 
										e.getMessage());
							}
							progress("allocating topology on each daemon",msg);
						}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(),
								TimeUnit.MILLISECONDS, (op3)->{
							op3.fail("timed out preparing the topology");		
						});
					} catch (DragonInvalidContext e) {
						client(new RunTopoErrorSMsg(msg.topologyId, e.getMessage()),msg);
					}
				}, (op, error) -> {
					client(new RunTopoErrorSMsg(msg.topologyId, error),msg);
				}).onRunning((op) -> {
					((PrepJarGroupOp) op).receiveSuccess(comms.getMyNodeDesc());
					progress("distributing topology jar file",msg);
				}).onTimeout(node.getTimer(),node.getConf().getDragonServiceTimeoutMs(),TimeUnit.MILLISECONDS,(op)->{
					op.fail("timed out distributing the topology");
				});
			} catch (DragonInvalidContext e) {
				client(new RunTopoErrorSMsg(msg.topologyId, e.getMessage()),msg);
			}

		}
	}

	/**
	 * Get the list of daemons that this daemon knows about.
	 * 
	 * @param msg
	 */
	private void processGetNodeContext(GetNodeContextSMsg msg) {
		NodeContext nc = new NodeContext();
		nc.putAll(node.getNodeProcessor().getContext());
		client(new NodeContextSMsg(nc), msg);
	}

	/**
	 * Get metrics for this daemon only.
	 * 
	 * @param msg
	 */
	private void processGetMetrics(GetMetricsSMsg msg) {
		if ((Boolean) node.getConf().getDragonMetricsEnabled()) {
			ComponentMetricMap cm = node.getMetrics(msg.topologyId);
			if (cm != null) {
				client(new MetricsSMsg(cm), msg);
			} else {
				client(new GetMetricsErrorSMsg("unknown topology or there are no samples available yet"), msg);
			}
		} else {
			log.warn("metrics are not enabled");
			client(new GetMetricsErrorSMsg("metrics are not enabled in dragon.yaml for this node"), msg);
		}
	}

	/**
	 * Terminate the topology, which consists of: - terminate the topology on all
	 * daemons - remove the topology from the router on all daemons
	 * 
	 * @param msg contains the name of the topology
	 */
	private void processTerminateTopology(TermTopoSMsg msg) {
		if (!node.getLocalClusters().containsKey(msg.topologyId)) {
			client(new TermTopoErrorSMsg(msg.topologyId, "topology does not exist"),msg);
		} else {
			final DragonTopology topology = node.getLocalClusters().get(msg.topologyId).getTopology();
			if(msg.purge) {
				try {
					Ops.inst().newRemoveTopoGroupOp(msg, topology, (op2) -> {
						client(new TopoTermdSMsg(msg.topologyId), msg);
					}, (op2, error) -> {
						client(new TermTopoErrorSMsg(msg.topologyId, error), msg);
					}).onRunning((op2) -> {
						try {
							node.removeTopo(msg.topologyId,msg.purge);
							((RemoveTopoGroupOp) op2).receiveSuccess(comms.getMyNodeDesc());
						} catch (DragonTopologyException e) {
							((RemoveTopoGroupOp) op2).receiveError(comms.getMyNodeDesc(),e.getMessage());
						}
						progress("waiting for up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds...",msg);
					}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op2)->{
						op2.fail("timed out removing topology from memory, possibly some machines are overloaded");
					});
				} catch (DragonInvalidContext e) {
					log.error("should not have an invalid context when purging: "+e.getMessage());
					e.printStackTrace();
					client(new TermTopoErrorSMsg(msg.topologyId, e.getMessage()), msg);
				}
			} else {
				try {
					Ops.inst().newTermTopoGroupOp(msg.topologyId, (op) -> {
						try {
							Ops.inst().newRemoveTopoGroupOp(msg, topology, (op2) -> {
								client(new TopoTermdSMsg(msg.topologyId), msg);
							}, (op2, error) -> {
								client(new TermTopoErrorSMsg(msg.topologyId, error), msg);
							}).onRunning((op2) -> {
								try {
									node.removeTopo(msg.topologyId,msg.purge);
									((RemoveTopoGroupOp) op2).receiveSuccess(comms.getMyNodeDesc());
								} catch (DragonTopologyException e) {
									((RemoveTopoGroupOp) op2).receiveError(comms.getMyNodeDesc(),e.getMessage());
								}
								progress("removing topology from memory",msg);
							}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op2)->{
								op2.fail("timed out removing topology from memory");
							});
						} catch (DragonInvalidContext e) {
							client(new TermTopoErrorSMsg(msg.topologyId, e.getMessage()), msg);
						}

					}, (op, error) -> {
						client(new TermTopoErrorSMsg(msg.topologyId, error), msg);
					}).onRunning((op) -> {
						try {
							// starts a thread to stop the topology
							node.terminateTopology(msg.topologyId, (TermTopoGroupOp) op);
						} catch (DragonTopologyException | DragonInvalidStateException e) {
							((TermTopoGroupOp) op).fail(e.getMessage());
						}
						progress("stopping spouts and waiting up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds for bolts to finish...", msg);
					}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op)->{
						progress("topology is not finishing, possibly mis-behaving user code... will try purging...",msg);
						op.cancel();
						msg.purge=true;
						try {
							Ops.inst().newRemoveTopoGroupOp(msg, topology, (op2) -> {
								client(new TopoTermdSMsg(msg.topologyId), msg);
							}, (op2, error) -> {
								client(new TermTopoErrorSMsg(msg.topologyId, error), msg);
							}).onRunning((op2) -> {
								try {
									node.removeTopo(msg.topologyId,false);
									((RemoveTopoGroupOp) op2).receiveSuccess(comms.getMyNodeDesc());
								} catch (DragonTopologyException e) {
									((RemoveTopoGroupOp) op2).receiveError(comms.getMyNodeDesc(),e.getMessage());
								}
								progress("removing topology from memory",msg);
							}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op2)->{
								op2.fail("timed out removing topology from memory");
							});
						} catch (DragonInvalidContext e) {
							log.error("should not have an invalid context when purging: "+e.getMessage());
							e.printStackTrace();
							client(new TermTopoErrorSMsg(msg.topologyId, e.getMessage()), msg);
						}
					});
				} catch (DragonInvalidContext e) {
					client(new TermTopoErrorSMsg(msg.topologyId, e.getMessage()), msg);
				}
			}
		}
	}

	/**
	 * List general information for all topologies running on all daemons.
	 * 
	 * @param msg
	 */
	private void processListTopologies(ListToposSMsg msg) {
		Ops.inst().newListToposGroupOp((op) -> {
			ListToposGroupOp ltgo = (ListToposGroupOp) op;
			client(new TopoListSMsg(ltgo.descState, ltgo.descErrors, ltgo.descComponents, ltgo.descMetrics), msg);
		}, (op, error) -> {
			client(new ListToposErrorSMsg(error), msg);
		}).onRunning((op) -> {
			ListToposGroupOp ltgo = (ListToposGroupOp) op;
			node.listTopologies(ltgo);
			ltgo.aggregate(comms.getMyNodeDesc(), ltgo.state, ltgo.errors, ltgo.components, ltgo.metrics);
			ltgo.receiveSuccess(comms.getMyNodeDesc());
			progress("waiting up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds...",msg);
		}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op)->{
			op.fail("timed out waiting for nodes to respond");
		});
	}

	/**
	 * Halt the topology. A halted topology can be resumed.
	 * 
	 * @param msg contains the name of the topology to halt.
	 */
	private void processHaltTopology(HaltTopoSMsg msg) {
		if (!node.getLocalClusters().containsKey(msg.topologyId)) {
			client(new HaltTopoErrorSMsg(msg.topologyId, "topology does not exist"),msg);
		} else {
			try {
				Ops.inst().newHaltTopoGroupOp(msg.topologyId, (op) -> {
					client(new TopoHaltedSMsg(msg.topologyId), msg);
				}, (op, error) -> {
					client(new HaltTopoErrorSMsg(msg.topologyId, error), msg);
				}).onRunning((op) -> {
					try {
						node.haltTopology(msg.topologyId);
						((HaltTopoGroupOp) op).receiveSuccess(comms.getMyNodeDesc());
						progress("waiting up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds...",msg);
					} catch (DragonTopologyException | DragonInvalidStateException e) {
						((HaltTopoGroupOp) op).receiveError(comms.getMyNodeDesc(),e.getMessage());
					}
				}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op)->{
					op.fail("timed out waiting for nodes to respond");
				});
			} catch (DragonInvalidContext e) {
				client(new HaltTopoErrorSMsg(msg.topologyId, e.getMessage()), msg);
			}
		}
	}

	/**
	 * Resume a halted topology.
	 * 
	 * @param msg contains the name of the topology to resume.
	 */
	private void processResumeTopology(ResumeTopoSMsg msg) {
		if (!node.getLocalClusters().containsKey(msg.topologyId)) {
			client(new ResumeTopoErrorSMsg(msg.topologyId, "topology does not exist"),msg);
		} else {
			try {
				Ops.inst().newResumeTopoGroupOp(msg.topologyId, (op) -> {
					client(new TopoResumedMsg(msg.topologyId), msg);
				}, (op, error) -> {
					client(new ResumeTopoErrorSMsg(msg.topologyId, error), msg);
				}).onRunning((op) -> {
					try {
						node.resumeTopology(msg.topologyId);
						((ResumeTopoGroupOp) op).receiveSuccess(comms.getMyNodeDesc());
						progress("waiting up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds...",msg);
					} catch (DragonTopologyException | DragonInvalidStateException e) {
						((ResumeTopoGroupOp) op).receiveError(comms.getMyNodeDesc(),e.getMessage());
					}
					
				}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op)->{
					op.fail("timed out waiting for nodes to respond");
				});
			} catch (DragonInvalidContext e) {
				client(new HaltTopoErrorSMsg(msg.topologyId, e.getMessage()), msg);
			}

		}
	}
	
	/**
	 * Allocate a new partition.
	 * 
	 * @param msg contains the partition name, number of daemons and strategy to use.
	 */
	private void processAllocatePartition(AllocPartSMsg msg) {
		final String partitionId = msg.partitionId;
		int number = msg.number;
		final NodeContext context = node.getNodeProcessor().getContext();
		
		/*
		 * Who will allocate new processes and how many.
		 */
		final HashMap<NodeDescriptor,Integer> allocation = new HashMap<>();
		
		/*
		 * The load of a machine.
		 */
		final HashMap<NodeDescriptor,Integer> load = new HashMap<>();
		
		/* list of machines to consider, with the primary node that will be contacted
		 * for the group operation
		 */
		final HashMap<String,NodeDescriptor> machines = new HashMap<>();
		
		// first build a list of machines, and designate a primary
		for(NodeDescriptor desc : context.values()) {
			if(!machines.containsKey(desc.getHostName())) {
				if(desc.isPrimary()) {
					machines.put(desc.getHostName(),desc);
				}
			}
		}
		// now find the load for each machine
		for(NodeDescriptor desc : context.values()) {
			String hostname = desc.getHostName();
			if(!load.containsKey(machines.get(hostname))){
				load.put(machines.get(hostname),1);
			} else {
				load.put(machines.get(hostname),
						load.get(machines.get(hostname))+1);
			}
		}
		
		if(partitionId.equals(Constants.DRAGON_PRIMARY_PARTITION)) {
			client(new AllocPartErrorSMsg(partitionId,0,"can not add to the primary partition"),msg);
			return;
		}
		
		switch(msg.strategy) {
		case BALANCED:
			PriorityQueue<NodeDescriptor> pQueue = 
				new PriorityQueue<NodeDescriptor>(load.size(),
						new Comparator<NodeDescriptor>() {
				@Override
				public int compare(NodeDescriptor arg0, NodeDescriptor arg1) {
					return load.get(machines.get(arg0.getHostName()))
							.compareTo(load.get(machines.get(arg1.getHostName())));
				}
			}); 
			for(NodeDescriptor host: load.keySet()) {
				pQueue.add(host);
			}
			while(number>0) {
				NodeDescriptor host = pQueue.remove();
				if(!allocation.containsKey(host)) {
					allocation.put(host,1);
				} else {
					allocation.put(host,allocation.get(host)+1);
				}
				load.put(host,load.get(host)+1);
				pQueue.add(host);
				number--;
			}
			break;
		case EACH:
			for(NodeDescriptor host : load.keySet()) {
				allocation.put(host,number);
			}
			break;
		case UNIFORM:
			while(number>0) {
				int inc = number>load.size() ? number/load.size():1;
				for(NodeDescriptor host : load.keySet()) {
					if(!allocation.containsKey(host)) {
						allocation.put(host,1);
					} else {
						allocation.put(host,allocation.get(host)+1);
					}
					number-=inc;
					if(number==0) break;
				}
			}
			break;
		default:
			client(new AllocPartErrorSMsg(partitionId,0,"invalid strategy ["+msg.strategy+"]"),msg);
			return;
		}
		Ops.inst().newAllocPartGroupOp(partitionId,allocation, (op)->{
			client(new PartAllocedSMsg(partitionId,0), msg);
		}, (op,error)->{
			client(new AllocPartErrorSMsg(partitionId,0,error),msg);
		}).onRunning((op)->{
			if(allocation.containsKey(node.getComms().getMyNodeDesc())) {
				int a = node.allocatePartition(partitionId, allocation.get(node.getComms().getMyNodeDesc()));
				AllocPartGroupOp apgo = (AllocPartGroupOp) op;
				if(a!=allocation.get(node.getComms().getMyNodeDesc())) {
					apgo.receiveError(comms.getMyNodeDesc(), "failed to allocate partitions on ["+node.getComms().getMyNodeDesc()+"]");
				} else {
					apgo.receiveSuccess(comms.getMyNodeDesc());
					progress("waiting up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds...",msg);
				}
			}
			
		}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op)->{
			op.fail("timed out waiting for nodes to respond");
		});
	}
	
	private void processDeallocatePartition(DeallocPartSMsg apsm) {
		final String partitionId = apsm.partitionId;
		int daemons = apsm.daemons;
		final NodeContext context = node.getNodeProcessor().getContext();
		// total partition counts
		final HashMap<String,Integer> partitionCount = new HashMap<>();
		// individual node codes
		final HashMap<NodeDescriptor,HashMap<String,Integer>> nodeCounts = new HashMap<>();
		// list of nodes that manage the partition, ordered by their individual counts
		final HashMap<String,PriorityQueue<NodeDescriptor>> pQueueMap = new HashMap<>();
		// list of deletions
		final HashMap<NodeDescriptor,Integer> deletions = new HashMap<>();
		
		if(partitionId.equals(Constants.DRAGON_PRIMARY_PARTITION)) {
			try {
				comms.sendServiceMsg(new DeallocPartErrorSMsg(partitionId,0,"can not delete the primary partition"),apsm);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
			return;
		}
		/**
		 * list of existing partitions with their parent nodes...
		 */
		for(NodeDescriptor desc : context.values()) {
			String part = desc.getPartition();
			NodeDescriptor parent = desc.getParent();
			if(!partitionCount.containsKey(part)) {
				partitionCount.put(part,1);
			} else {
				partitionCount.put(part,partitionCount.get(part)+1);
			}
			if(!nodeCounts.containsKey(parent)) {
				nodeCounts.put(parent,new HashMap<>());
			}
			if(!nodeCounts.get(parent).containsKey(part)) {
				nodeCounts.get(parent).put(part,1);
			} else {
				nodeCounts.get(parent).put(part,nodeCounts.get(parent).get(part)+1);
			}
			
		}
		for(NodeDescriptor desc : context.values()) {
			String part = desc.getPartition();
			if(!part.equals(Constants.DRAGON_PRIMARY_PARTITION)) {
				if(!pQueueMap.containsKey(part)) {
					pQueueMap.put(part,new PriorityQueue<NodeDescriptor>(
							new Comparator<NodeDescriptor>() {
						@Override
						public int compare(NodeDescriptor arg0, NodeDescriptor arg1) {
							return nodeCounts.get(arg1).get(part).compareTo(nodeCounts.get(arg0).get(part));
						}
					}));
				}
				pQueueMap.get(part).add(desc.getParent());
			}
		}
		
		if(!partitionCount.containsKey(partitionId)) {
			client(new DeallocPartErrorSMsg(partitionId,0,"partition does not exist ["+partitionId+"]"),apsm);
			return;
		}
		if(partitionCount.get(partitionId)<daemons) {
			client(new DeallocPartErrorSMsg(partitionId,0,"only ["+partitionCount.get(partitionId)+"] partition instances exist"),apsm);
			return;
		}
		switch(apsm.strategy) {
		case BALANCED:
			while(daemons>0 && partitionCount.get(partitionId)>0) {
				NodeDescriptor desc = pQueueMap.get(partitionId).poll();
				nodeCounts.get(desc).put(partitionId,nodeCounts.get(desc).get(partitionId)-1);
				partitionCount.put(partitionId,partitionCount.get(partitionId)-1);
				daemons--;
				if(!deletions.containsKey(desc)) {
					deletions.put(desc,1);
				} else {
					deletions.put(desc,deletions.get(desc)+1);
				}
			}
			break;
		case EACH:
			for(NodeDescriptor host : nodeCounts.keySet()) {
				if(nodeCounts.get(host).containsKey(partitionId) &&
						nodeCounts.get(host).get(partitionId)>0) {
					int amount = Math.min(nodeCounts.get(host).get(partitionId),daemons);
					deletions.put(host,amount);
				}
			}
			break;
		case UNIFORM:
			while(daemons>0) {
				int inc = daemons>partitionCount.get(partitionId) 
						? daemons/partitionCount.get(partitionId):1;
				for(NodeDescriptor host : nodeCounts.keySet()) {
					if(nodeCounts.get(host).get(partitionId)>0) {
						if(!deletions.containsKey(host)) {
							deletions.put(host,1);
						} else {
							deletions.put(host,deletions.get(host)+1);
						}
						daemons-=inc;
						if(daemons==0) break;
					}
				}
			}
			break;
		default:
			client(new DeallocPartErrorSMsg(partitionId,0,"invalid strategy ["+apsm.strategy+"]"),apsm);
			return;
		}
		Ops.inst().newDeallocPartGroupOp(partitionId,deletions, (op)->{
			client(new PartDeallocedSMsg(partitionId,0), apsm);
		}, (op,error)->{
			client(new DeallocPartErrorSMsg(partitionId,0,error),apsm);
		}).onRunning((op)->{
			if(deletions.containsKey(node.getComms().getMyNodeDesc())) {
				int a = node.deallocatePartition(partitionId, deletions.get(node.getComms().getMyNodeDesc()));
				DeallocPartGroupOp apgo = (DeallocPartGroupOp) op;
				if(a!=deletions.get(node.getComms().getMyNodeDesc())) {
					apgo.receiveError(comms.getMyNodeDesc(), "failed to delete partition on ["+node.getComms().getMyNodeDesc()+"]");
				} else {
					apgo.receiveSuccess(comms.getMyNodeDesc());
					progress("waiting up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds...",apsm);
				}
			}
			
		}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op)->{
			op.fail("timed out waiting for nodes to respond");
		});
	}

	/**
	 * Get the status of the daemons.
	 * @param msg
	 */
	private void processGetStatus(ServiceMessage msg) {
		GetStatusSMsg gssm = (GetStatusSMsg) msg;
		Ops.inst().newGetStatusGroupOp((op)->{
			GetStatusGroupOp gsgo = (GetStatusGroupOp) op;
			client(new StatusSMsg(gsgo.dragonStatus),gssm);
		}, (op,error)->{
			client(new GetStatusErrorSMsg((String)error),gssm);
		}).onRunning((op)->{
			GetStatusGroupOp gsgo = (GetStatusGroupOp) op;
			NodeStatus nodeStatus = node.getStatus();
			nodeStatus.context=node.getNodeProcessor().getContext();
			gsgo.aggregate(nodeStatus);
			gsgo.receiveSuccess(comms.getMyNodeDesc());
			progress("waiting up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds...",msg);
		}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op)->{
			op.fail("timed out waiting for nodes to respond");
		});
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		log.info("starting up");
		while (!isInterrupted()) {
			ServiceMessage msg;
			try {
				msg = node.getComms().receiveServiceMsg();
			} catch (InterruptedException e) {
				log.info("interrupted");
				break;
			}
			// do when appropriate
			node.getOpsProcessor().newConditionOp((op)->{
				return node.getNodeState()==NodeState.OPERATIONAL;
			},(op)->{
				try {
					node.getOperationsLock().lockInterruptibly();
					switch (msg.getType()) {
					case UPLOAD_JAR:
						processUploadJar((UploadJarSMsg) msg);
						break;
					case RUN_TOPOLOGY:
						processRunTopology((RunTopoSMsg) msg);
						break;
					case GET_NODE_CONTEXT:
						processGetNodeContext((GetNodeContextSMsg) msg);
						break;
					case GET_METRICS:
						processGetMetrics((GetMetricsSMsg) msg);
						break;
					case TERMINATE_TOPOLOGY:
						processTerminateTopology((TermTopoSMsg) msg);
						break;
					case LIST_TOPOLOGIES:
						processListTopologies((ListToposSMsg) msg);
						break;
					case HALT_TOPOLOGY:
						processHaltTopology((HaltTopoSMsg) msg);
						break;
					case RESUME_TOPOLOGY:
						processResumeTopology((ResumeTopoSMsg) msg);
						break;
					case ALLOCATE_PARTITION:
						processAllocatePartition((AllocPartSMsg) msg);
						break;
					case DEALLOCATE_PARTITION:
						processDeallocatePartition((DeallocPartSMsg) msg);
						break;
					case GET_STATUS:
						processGetStatus(msg);
						break;
					default:
						log.error("unrecognized command: " + msg.getType().name());
					}
				} catch (InterruptedException e) {
					log.error("interrupted while waiting for node operations lock");
				} finally {
					node.getOperationsLock().unlock();
				}
			}, (op,error)->{
				log.error(error);
			});
			
		}
		log.info("shutting down");
	}
}
