package dragon.network;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import dragon.DragonRequiresClonableException;
import dragon.metrics.ComponentMetricMap;
import dragon.network.messages.service.RunTopoSMsg;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TermTopoErrorSMsg;
import dragon.network.messages.service.TermTopoSMsg;
import dragon.network.messages.service.TopoHaltedSMsg;
import dragon.network.messages.service.TopoListSMsg;
import dragon.network.messages.service.TopoResumedMsg;
import dragon.network.messages.service.TopoRunningSMsg;
import dragon.network.messages.service.TopoTermdSMsg;
import dragon.network.messages.service.UploadJarFailedSMsg;
import dragon.network.messages.service.RunTopoErrorSMsg;
import dragon.network.comms.DragonCommsException;
import dragon.network.comms.IComms;
import dragon.network.messages.service.GetMetricsSMsg;
import dragon.network.messages.service.HaltTopoErrorSMsg;
import dragon.network.messages.service.HaltTopoSMsg;
import dragon.network.messages.service.UploadJarSMsg;
import dragon.network.messages.service.UploadJarSuccessSMsg;
import dragon.network.operations.HaltTopoGroupOp;
import dragon.network.operations.ListToposGroupOp;
import dragon.network.operations.PrepareTopoGroupOp;
import dragon.network.operations.ResumeTopoGroupOp;
import dragon.network.operations.RunTopoGroupOp;
import dragon.network.operations.StartTopoGroupOp;
import dragon.network.operations.RemoveTopoGroupOp;
import dragon.network.operations.TermTopoGroupOp;
import dragon.network.operations.Ops;
import dragon.topology.DragonTopology;
import dragon.network.messages.service.AllocPartErrorSMsg;
import dragon.network.messages.service.AllocPartSMsg;
import dragon.network.messages.service.GetMetricsErrorSMsg;
import dragon.network.messages.service.MetricsSMsg;
import dragon.network.messages.service.NodeContextSMsg;
import dragon.network.messages.service.PartAllocedSMsg;
import dragon.network.messages.service.ResumeTopoErrorSMsg;
import dragon.network.messages.service.ResumeTopoSMsg;

/**
 * Process service messages that come from a client. Processing may require
 * issuing a sequence of group operations. This object will read service
 * messages one at time from the Comms layer.
 * 
 * @author aaron
 *
 */
public class ServiceProcessor extends Thread {
	private final static Logger log = LogManager.getLogger(ServiceProcessor.class);
	private final Node node;
	private final IComms comms;

	public ServiceProcessor(Node node) {
		this.node = node;
		this.comms = node.getComms();
		setName("service processor");
		log.info("starting service processor");
		start();
	}

	/**
	 * Upload a JAR file to the daemon. JAR files that contain the topology must be
	 * uploaded prior to the run topology commmand, on the same daemon that the run
	 * topology command will be given to.
	 * 
	 * @param msg contains the name of the topology and the JAR byte array
	 */
	private void processUploadJar(ServiceMessage msg) {
		UploadJarSMsg jf = (UploadJarSMsg) msg;
		if (node.getLocalClusters().containsKey(jf.topologyId)) {
			try {
				comms.sendServiceMsg(new UploadJarFailedSMsg(jf.topologyId, "topology exists"), jf);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		} else {
			log.info("storing topology [" + jf.topologyId + "]");
			if (!node.storeJarFile(jf.topologyId, jf.topologyJar)) {
				try {
					comms.sendServiceMsg(
							new UploadJarFailedSMsg(jf.topologyId, "could not store the topology jar"), jf);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
				return;
			}
			if (!node.loadJarFile(jf.topologyId)) {
				try {
					comms.sendServiceMsg(
							new UploadJarFailedSMsg(jf.topologyId, "could not load the topology jar"), jf);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
				return;
			}
			try {
				comms.sendServiceMsg(new UploadJarSuccessSMsg(jf.topologyId), jf);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		}
	}

	/**
	 * Run the topology. Running a topology involves the sequence of group
	 * operations: - send the jar file to all daemons - allocate the local cluster
	 * on all daemons - start the local cluster on all daemons
	 * 
	 * @param msg contains the name of the topology and the topology itself
	 */
	private void processRunTopology(ServiceMessage msg) {
		RunTopoSMsg rtm = (RunTopoSMsg) msg;
		if (node.getLocalClusters().containsKey(rtm.topologyId)) {
			try {
				comms.sendServiceMsg(new RunTopoErrorSMsg(rtm.topologyId, "topology exists"),
						rtm);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		} else {
			final DragonTopology topo = rtm.dragonTopology;
			byte[] jarfile =  node.readJarFile(rtm.topologyId);
			if(jarfile==null){
				try {
					comms.sendServiceMsg(new RunTopoErrorSMsg(rtm.topologyId, "could not read the jar file"),rtm);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
				return;
			}
			Ops.inst().newRunTopoGroupOp(rtm.topologyId, jarfile, topo, (op) -> {
				Ops.inst().newPrepareTopoGroupOp(rtm, topo, (op2) -> {
					Ops.inst().newStartTopologyGroupOp(rtm.topologyId, (op3) -> {
						try {
							comms.sendServiceMsg(new TopoRunningSMsg(rtm.topologyId),
									rtm);
						} catch (DragonCommsException e) {
							log.fatal("can't communicate with client: " + e.getMessage());
						}
					}, (op3, error) -> {
						try {
							comms.sendServiceMsg(new RunTopoErrorSMsg(rtm.topologyId, error),
									rtm);
						} catch (DragonCommsException e) {
							log.fatal("can't communicate with client: " + e.getMessage());
						}
					}).onRunning((op3) -> {
						try {
							node.startTopology(rtm.topologyId);
						} catch (DragonTopologyException e) {
							((StartTopoGroupOp)op3).fail(e.getMessage());
						}
						((StartTopoGroupOp) op3).receiveSuccess(comms,comms.getMyNodeDesc());
					});
				}, (op2, error) -> {
					try {
						comms.sendServiceMsg(new RunTopoErrorSMsg(rtm.topologyId, error),
								rtm);
					} catch (DragonCommsException e) {
						log.fatal("can't communicate with client: " + e.getMessage());
					}
				}).onRunning((op2) -> {
					try {
						node.prepareTopology(rtm.topologyId, rtm.conf, topo, false);
						((PrepareTopoGroupOp) op2).receiveSuccess(comms,comms.getMyNodeDesc());
					} catch (DragonRequiresClonableException | DragonTopologyException e) {
						((PrepareTopoGroupOp) op2).receiveError(comms,comms.getMyNodeDesc(), 
								e.getMessage());
					}
				});
			}, (op, error) -> {
				try {
					comms.sendServiceMsg(new RunTopoErrorSMsg(rtm.topologyId, error),rtm);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
			}).onRunning((op) -> {
				((RunTopoGroupOp) op).receiveSuccess(comms, comms.getMyNodeDesc());
			});

		}
	}

	/**
	 * Get the list of daemons that this daemon knows about.
	 * 
	 * @param msg
	 */
	private void processGetNodeContext(ServiceMessage msg) {
		try {
			NodeContext nc = new NodeContext();
			nc.putAll(node.getNodeProcessor().getContext());
			comms.sendServiceMsg(new NodeContextSMsg(nc), msg);
		} catch (DragonCommsException e) {
			log.fatal("can't communicate with client: " + e.getMessage());
		}
	}

	/**
	 * Get metrics for this daemon only.
	 * 
	 * @param msg
	 */
	private void processGetMetrics(ServiceMessage msg) {
		GetMetricsSMsg gm = (GetMetricsSMsg) msg;
		if ((Boolean) node.getConf().getDragonMetricsEnabled()) {
			ComponentMetricMap cm = node.getMetrics(gm.topologyId);
			if (cm != null) {
				try {
					comms.sendServiceMsg(new MetricsSMsg(cm), msg);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
			} else {
				try {
					comms.sendServiceMsg(
							new GetMetricsErrorSMsg("unknown topology or there are no samples available yet"), msg);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
			}
		} else {
			log.warn("metrics are not enabled");
			try {
				comms.sendServiceMsg(
						new GetMetricsErrorSMsg("metrics are not enabled in dragon.yaml for this node"), msg);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		}
	}

	/**
	 * Terminate the topology, which consists of: - terminate the topology on all
	 * daemons - remove the topology from the router on all daemons
	 * 
	 * @param msg contains the name of the topology
	 */
	private void processTerminateTopology(ServiceMessage msg) {
		TermTopoSMsg tt = (TermTopoSMsg) msg;
		if (!node.getLocalClusters().containsKey(tt.topologyId)) {
			try {
				comms.sendServiceMsg(new TermTopoErrorSMsg(tt.topologyId, "topology does not exist"),
						msg);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		} else {
			final DragonTopology topology = node.getLocalClusters().get(tt.topologyId).getTopology();
			Ops.inst().newTermTopoGroupOp(tt.topologyId, (op) -> {
				Ops.inst().newRemoveTopoGroupOp(tt, topology, (op2) -> {
					try {
						comms.sendServiceMsg(new TopoTermdSMsg(tt.topologyId), tt);
					} catch (DragonCommsException e) {
						log.fatal("can't communicate with client: " + e.getMessage());
					}
				}, (op2, error) -> {
					try {
						comms.sendServiceMsg(new TermTopoErrorSMsg(tt.topologyId, error), tt);
					} catch (DragonCommsException e) {
						log.fatal("can't communicate with client: " + e.getMessage());
					}
				}).onRunning((op2) -> {
					try {
						node.removeTopo(tt.topologyId);
						((RemoveTopoGroupOp) op2).receiveSuccess(comms, comms.getMyNodeDesc());
					} catch (DragonTopologyException e) {
						((RemoveTopoGroupOp) op2).receiveError(comms, comms.getMyNodeDesc(),e.getMessage());
					}
					
				});

			}, (op, error) -> {
				try {
					comms.sendServiceMsg(new TermTopoErrorSMsg(tt.topologyId, error), tt);
				} catch (DragonCommsException e) {
					log.error("could not send terminate topology error message");
				}
			}).onRunning((op) -> {
				try {
					// starts a thread to stop the topology
					node.terminateTopology(tt.topologyId, (TermTopoGroupOp) op);
				} catch (DragonTopologyException e) {
					((TermTopoGroupOp) op).fail(e.getMessage());
				} 
			});

		}
	}

	/**
	 * List general information for all topologies running on all daemons.
	 * 
	 * @param msg
	 */
	private void processListTopologies(ServiceMessage msg) {
		Ops.inst().newListToposGroupOp((op) -> {
			ListToposGroupOp ltgo = (ListToposGroupOp) op;
			try {
				comms.sendServiceMsg(new TopoListSMsg(ltgo.descState, ltgo.descErrors), msg);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		}, (op, error) -> {
			log.fatal(error);
		}).onRunning((op) -> {
			ListToposGroupOp ltgo = (ListToposGroupOp) op;
			node.listTopologies(ltgo);
			ltgo.aggregate(comms.getMyNodeDesc(), ltgo.state, ltgo.errors);
			ltgo.sendSuccess(comms);
		});
	}

	/**
	 * Halt the topology. A halted topology can be resumed.
	 * 
	 * @param msg contains the name of the topology to halt.
	 */
	private void processHaltTopology(ServiceMessage msg) {
		HaltTopoSMsg htm = (HaltTopoSMsg) msg;
		if (!node.getLocalClusters().containsKey(htm.topologyId)) {
			try {
				comms.sendServiceMsg(new HaltTopoErrorSMsg(htm.topologyId, "topology does not exist"),
						msg);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		} else {
			Ops.inst().newHaltTopoGroupOp(htm.topologyId, (op) -> {
				try {
					comms.sendServiceMsg(new TopoHaltedSMsg(htm.topologyId), msg);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
			}, (op, error) -> {
				try {
					comms.sendServiceMsg(new HaltTopoErrorSMsg(htm.topologyId, error), msg);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
			}).onRunning((op) -> {
				try {
					node.haltTopology(htm.topologyId);
					((HaltTopoGroupOp) op).receiveSuccess(comms, comms.getMyNodeDesc());
				} catch (DragonTopologyException e) {
					((HaltTopoGroupOp) op).receiveError(comms, comms.getMyNodeDesc(),e.getMessage());
				}
				
			});
		}
	}

	/**
	 * Resume a halted topology.
	 * 
	 * @param msg contains the name of the topology to resume.
	 */
	private void processResumeTopology(ServiceMessage msg) {
		ResumeTopoSMsg htm = (ResumeTopoSMsg) msg;
		if (!node.getLocalClusters().containsKey(htm.topologyId)) {
			try {
				comms.sendServiceMsg(new ResumeTopoErrorSMsg(htm.topologyId, "topology does not exist"),
						msg);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		} else {
			Ops.inst().newResumeTopoGroupOp(htm.topologyId, (op) -> {
				try {
					comms.sendServiceMsg(new TopoResumedMsg(htm.topologyId), msg);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
			}, (op, error) -> {
				try {
					comms.sendServiceMsg(new ResumeTopoErrorSMsg(htm.topologyId, error), msg);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
			}).onRunning((op) -> {
				try {
					node.resumeTopology(htm.topologyId);
					((ResumeTopoGroupOp) op).receiveSuccess(comms, comms.getMyNodeDesc());
				} catch (DragonTopologyException e) {
					((ResumeTopoGroupOp) op).receiveError(comms, comms.getMyNodeDesc(),e.getMessage());
				}
				
			});

		}
	}
	
	/**
	 * Allocate a new partition.
	 * 
	 * @param msg contains the partition name, number of daemons and strategy to use.
	 */
	private void processAllocatePartition(ServiceMessage msg) {
		final AllocPartSMsg apsm = (AllocPartSMsg) msg;
		final String partitionId = apsm.partitionId;
		int daemons = apsm.daemons;
		final NodeContext context = node.getNodeProcessor().getContext();
		final HashMap<NodeDescriptor,Integer> allocation = new HashMap<NodeDescriptor,Integer>();
		final HashMap<NodeDescriptor,Integer> load = new HashMap<NodeDescriptor,Integer>();
		final HashMap<String,NodeDescriptor> machines = new HashMap<String,NodeDescriptor>();
		for(NodeDescriptor desc : context.values()) {
			if(desc.getPartition().equals(partitionId)) {
				try {
					comms.sendServiceMsg(new AllocPartErrorSMsg(partitionId,0,"partition already exists"),msg);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
				return;
			}
			if(!machines.containsKey(desc.getHostName())){
				if(desc.isPrimary()) {
					machines.put(desc.getHostName(),desc);
				}
				load.put(machines.get(desc.getHostName()),1);
			} else {
				load.put(machines.get(desc.getHostName()),
						load.get(machines.get(desc.getHostName()))+1);
			}
		}
		switch(apsm.strategy) {
		case LEAST_LOADED:
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
			while(daemons>0) {
				NodeDescriptor host = pQueue.remove();
				if(!allocation.containsKey(host)) {
					allocation.put(host,1);
				} else {
					allocation.put(host,allocation.get(host)+1);
				}
				load.put(host,load.get(host)+1);
				pQueue.add(host);
				daemons--;
			}
			break;
		case PER_PRIMARY:
			for(NodeDescriptor host : load.keySet()) {
				allocation.put(host,daemons);
			}
			break;
		case UNIFORMLY:
			while(daemons>0) {
				int inc = daemons>load.size() ? daemons/load.size():1;
				for(NodeDescriptor host : load.keySet()) {
					if(!allocation.containsKey(host)) {
						allocation.put(host,1);
					} else {
						allocation.put(host,allocation.get(host)+1);
					}
					daemons-=inc;
					if(daemons==0) break;
				}
			}
			break;
		default:
			try {
				comms.sendServiceMsg(new AllocPartErrorSMsg(partitionId,0,"invalid strategy ["+apsm.strategy+"]"),msg);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
			return;
		}
		
		Ops.inst().newAllocPartGroupOp(partitionId,allocation, (op)->{
			try {
				comms.sendServiceMsg(new PartAllocedSMsg(partitionId,0), msg);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		}, (op,error)->{
			try {
				comms.sendServiceMsg(new AllocPartErrorSMsg(partitionId,0,error),msg);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		}).onStart((op)->{
			
		});
	}

	@Override
	public void run() {
		while (!isInterrupted()) {
			ServiceMessage msg;
			try {
				msg = node.getComms().receiveServiceMsg();
			} catch (InterruptedException e) {
				log.info("interrupted");
				break;
			}
			switch (msg.getType()) {
			case UPLOAD_JAR:
				processUploadJar(msg);
				break;
			case RUN_TOPOLOGY:
				processRunTopology(msg);
				break;
			case GET_NODE_CONTEXT:
				processGetNodeContext(msg);
				break;
			case GET_METRICS:
				processGetMetrics(msg);
				break;
			case TERMINATE_TOPOLOGY:
				processTerminateTopology(msg);
				break;
			case LIST_TOPOLOGIES:
				processListTopologies(msg);
				break;
			case HALT_TOPOLOGY:
				processHaltTopology(msg);
				break;
			case RESUME_TOPOLOGY:
				processResumeTopology(msg);
				break;
			case ALLOCATE_PARTITION:
				processAllocatePartition(msg);
				break;
			default:
				log.error("unrecognized command: " + msg.getType().name());
			}
		}
		log.info("shut down");
	}
}
