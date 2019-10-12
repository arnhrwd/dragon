package dragon.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
import dragon.network.operations.TermRouterGroupOp;
import dragon.network.operations.TermTopoGroupOp;
import dragon.network.operations.Operations;
import dragon.topology.DragonTopology;
import dragon.network.messages.service.GetMetricsErrorSMsg;
import dragon.network.messages.service.MetricsSMsg;
import dragon.network.messages.service.NodeContextSMsg;
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
	private final static Log log = LogFactory.getLog(ServiceProcessor.class);
	private boolean shouldTerminate = false;
	private final Node node;

	public ServiceProcessor(Node node) {
		this.node = node;
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
		if (node.getLocalClusters().containsKey(jf.topologyName)) {
			try {
				node.getComms().sendServiceMsg(new UploadJarFailedSMsg(jf.topologyName, "topology exists"), jf);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		} else {
			log.info("storing topology [" + jf.topologyName + "]");
			if (!node.storeJarFile(jf.topologyName, jf.topologyJar)) {
				try {
					node.getComms().sendServiceMsg(
							new UploadJarFailedSMsg(jf.topologyName, "could not store the topology jar"), jf);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
				return;
			}
			if (!node.loadJarFile(jf.topologyName)) {
				try {
					node.getComms().sendServiceMsg(
							new UploadJarFailedSMsg(jf.topologyName, "could not load the topology jar"), jf);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
				return;
			}
			try {
				node.getComms().sendServiceMsg(new UploadJarSuccessSMsg(jf.topologyName), jf);
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
				node.getComms().sendServiceMsg(new RunTopoErrorSMsg(rtm.topologyId, "topology exists"),
						rtm);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		} else {
			final DragonTopology topo = rtm.dragonTopology;
			Operations.getInstance()
				.newRunTopoGroupOp(rtm, node.readJarFile(rtm.topologyId), topo, (op) -> {
					Operations.getInstance().newPrepareTopoGroupOp(rtm, topo, (op2) -> {
						Operations.getInstance().newStartTopologyGroupOperation(rtm, (op3) -> {
							try {
								node.getComms().sendServiceMsg(new TopoRunningSMsg(rtm.topologyId),
										rtm);
							} catch (DragonCommsException e) {
								log.fatal("can't communicate with client: " + e.getMessage());
							}
						}, (op3, error) -> {
							try {
								node.getComms().sendServiceMsg(new RunTopoErrorSMsg(rtm.topologyId, error),
										rtm);
							} catch (DragonCommsException e) {
								log.fatal("can't communicate with client: " + e.getMessage());
							}
						}).onRunning((op3) -> {
							node.startTopology(rtm.topologyId);
							((StartTopoGroupOp) op3).receiveSuccess(node.getComms(),
									node.getComms().getMyNodeDesc());
						});
					}, (op2, error) -> {
						try {
							node.getComms().sendServiceMsg(new RunTopoErrorSMsg(rtm.topologyId, error),
									rtm);
						} catch (DragonCommsException e) {
							log.fatal("can't communicate with client: " + e.getMessage());
						}
					}).onRunning((op2) -> {
						try {
							node.prepareTopology(rtm.topologyId, rtm.conf, topo, false);
							((PrepareTopoGroupOp) op2).receiveSuccess(node.getComms(),
									node.getComms().getMyNodeDesc());
						} catch (DragonRequiresClonableException e) {
							((PrepareTopoGroupOp) op2).receiveError(node.getComms(),
									node.getComms().getMyNodeDesc(), e.getMessage());
						}
					});
				}, (op, error) -> {
					try {
						node.getComms().sendServiceMsg(new RunTopoErrorSMsg(rtm.topologyId, error),
								rtm);
					} catch (DragonCommsException e) {
						log.fatal("can't communicate with client: " + e.getMessage());
					}
				}).onRunning((op) -> {
					((RunTopoGroupOp) op).receiveSuccess(node.getComms(), node.getComms().getMyNodeDesc());
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
			node.getComms().sendServiceMsg(new NodeContextSMsg(node.getNodeProcessor().getContext()), msg);
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
					node.getComms().sendServiceMsg(new MetricsSMsg(cm), msg);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
			} else {
				try {
					node.getComms().sendServiceMsg(
							new GetMetricsErrorSMsg("unknown topology or there are no samples available yet"), msg);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
			}
		} else {
			log.warn("metrics are not enabled");
			try {
				node.getComms().sendServiceMsg(
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
				node.getComms().sendServiceMsg(new TermTopoErrorSMsg(tt.topologyId, "topology does not exist"),
						msg);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		} else {
			final DragonTopology topology = node.getLocalClusters().get(tt.topologyId).getTopology();
			Operations.getInstance().newTermTopoGroupOp(tt, (op) -> {
				Operations.getInstance().newTermRouterGroupOp(tt, topology, (op2) -> {
					try {
						node.getComms().sendServiceMsg(new TopoTermdSMsg(tt.topologyId), tt);
					} catch (DragonCommsException e) {
						log.fatal("can't communicate with client: " + e.getMessage());
					}
				}, (op2, error) -> {
					try {
						node.getComms().sendServiceMsg(new TermTopoErrorSMsg(tt.topologyId, error), tt);
					} catch (DragonCommsException e) {
						log.fatal("can't communicate with client: " + e.getMessage());
					}
				}).onRunning((op2) -> {
					node.removeTopo(tt.topologyId);
					((TermRouterGroupOp) op2).receiveSuccess(node.getComms(), node.getComms().getMyNodeDesc());
				});

			}, (op, error) -> {
				try {
					node.getComms().sendServiceMsg(new TermTopoErrorSMsg(tt.topologyId, error), tt);
				} catch (DragonCommsException e) {
					log.error("could not send terminate topology error message");
				}
			}).onRunning((op) -> {
				node.stopTopology(tt.topologyId, (TermTopoGroupOp) op); // starts a thread to stop the topology
				// can't send success for this node until the topology has actually stopped
			});

		}
	}

	/**
	 * List general information for all topologies running on all daemons.
	 * 
	 * @param msg
	 */
	private void processListTopologies(ServiceMessage msg) {
		Operations.getInstance().newListToposGroupOp((op) -> {
			ListToposGroupOp ltgo = (ListToposGroupOp) op;
			try {
				node.getComms().sendServiceMsg(new TopoListSMsg(ltgo.descState, ltgo.descErrors), msg);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		}, (op, error) -> {
			log.fatal(error);
		}).onRunning((op) -> {
			ListToposGroupOp ltgo = (ListToposGroupOp) op;
			node.listTopologies(ltgo);
			ltgo.aggregate(node.getComms().getMyNodeDesc(), ltgo.state, ltgo.errors);
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
				node.getComms().sendServiceMsg(new HaltTopoErrorSMsg(htm.topologyId, "topology does not exist"),
						msg);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		} else {
			Operations.getInstance().newHaltTopoGroupOp(htm, (op) -> {
				try {
					node.getComms().sendServiceMsg(new TopoHaltedSMsg(htm.topologyId), msg);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
			}, (op, error) -> {
				try {
					node.getComms().sendServiceMsg(new HaltTopoErrorSMsg(htm.topologyId, error), msg);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
			}).onRunning((op) -> {
				node.haltTopology(htm.topologyId);
				((HaltTopoGroupOp) op).receiveSuccess(node.getComms(), node.getComms().getMyNodeDesc());
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
				node.getComms().sendServiceMsg(new ResumeTopoErrorSMsg(htm.topologyId, "topology does not exist"),
						msg);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
		} else {
			Operations.getInstance().newResumeTopoGroupOp(htm, (op) -> {
				try {
					node.getComms().sendServiceMsg(new TopoResumedMsg(htm.topologyId), msg);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
			}, (op, error) -> {
				try {
					node.getComms().sendServiceMsg(new ResumeTopoErrorSMsg(htm.topologyId, error), msg);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: " + e.getMessage());
				}
			}).onRunning((op) -> {
				node.resumeTopology(htm.topologyId);
				((ResumeTopoGroupOp) op).receiveSuccess(node.getComms(), node.getComms().getMyNodeDesc());
			});

		}
	}

	@Override
	public void run() {
		while (!shouldTerminate) {
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
			default:
				log.error("unrecognized command: " + msg.getType().name());
			}
		}
	}
}
