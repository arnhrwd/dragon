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
import dragon.network.messages.service.TopologyTerminatedMessage;
import dragon.network.messages.service.UploadJarFailedSMsg;
import dragon.network.messages.service.RunTopoErrorSMsg;
import dragon.network.comms.DragonCommsException;
import dragon.network.messages.service.GetMetricsSMsg;
import dragon.network.messages.service.HaltTopoErrorSMsg;
import dragon.network.messages.service.HaltTopoSMsg;
import dragon.network.messages.service.ListToposSMsg;
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
 * Process service messages that come from a client. Processing may require issuing a
 * sequence of group operations. This object will read service messages one
 * at time from the Comms layer.
 * @author aaron
 *
 */
public class ServiceProcessor extends Thread {
	private final static Log log = LogFactory.getLog(ServiceProcessor.class);
	private boolean shouldTerminate=false;
	private final Node node;
	public ServiceProcessor(Node node) {
		this.node=node;
		log.info("starting service processor");
		start();
	}
	
	/**
	 * Upload a JAR file to the daemon. JAR files that contain the topology must
	 * be uploaded prior to the run topology commmand, on the same daemon that
	 * the run topology command will be given to.
	 * @param command contains the name of the topology and the JAR byte array
	 */
	private void processUploadJar(ServiceMessage command){
		UploadJarSMsg jf = (UploadJarSMsg) command;
		if(node.getLocalClusters().containsKey(jf.topologyName)){
			try {
				node.getComms().sendServiceMessage(new UploadJarFailedSMsg(jf.topologyName,"topology exists"),jf);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: "+e.getMessage());
			}
		} else {
			log.info("storing topology ["+jf.topologyName+"]");
			if(!node.storeJarFile(jf.topologyName,jf.topologyJar)) {
				try {
					node.getComms().sendServiceMessage(new UploadJarFailedSMsg(jf.topologyName,"could not store the topology jar"),jf);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: "+e.getMessage());
				}
				return;
			}
			if(!node.loadJarFile(jf.topologyName)) {
				try {
					node.getComms().sendServiceMessage(new UploadJarFailedSMsg(jf.topologyName,"could not load the topology jar"),jf);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: "+e.getMessage());
				}				
				return;
			}
			try {
				node.getComms().sendServiceMessage(new UploadJarSuccessSMsg(jf.topologyName),jf);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: "+e.getMessage());
			}
		}
	}
	
	/**
	 * Run the topology. Running a topology involves the sequence of group operations:
	 * - send the jar file to all daemons
	 * - allocate the local cluster on all daemons
	 * - start the local cluster on all daemons
	 * @param command contains the name of the topology and the topology itself
	 */
	private void processRunTopology(ServiceMessage command) {
		RunTopoSMsg scommand = (RunTopoSMsg) command;
		if(node.getLocalClusters().containsKey(scommand.topologyName)){
			try {
				node.getComms().sendServiceMessage(new RunTopoErrorSMsg(scommand.topologyName,"topology exists"),scommand);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: "+e.getMessage());
			}
		} else {
			final DragonTopology dragonTopology = scommand.dragonTopology;
			Operations.getInstance().newRunTopoGroupOp(scommand,
				node.readJarFile(scommand.topologyName), dragonTopology, (op) -> {
					log.debug("here");
					scommand.dragonTopology = dragonTopology; // undo a side-effect from the run topology operation
					Operations.getInstance().newPrepareTopoGroupOp(scommand, dragonTopology, (op2) -> {
						Operations.getInstance().newStartTopologyGroupOperation(scommand, (op3) -> {
								try {
									node.getComms().sendServiceMessage(
											new TopoRunningSMsg(scommand.topologyName), scommand);
								} catch (DragonCommsException e) {
									log.fatal("can't communicate with client: " + e.getMessage());
								}
							}, (op3, error) -> {
								try {
									node.getComms().sendServiceMessage(
											new RunTopoErrorSMsg(scommand.topologyName, error),
											scommand);
								} catch (DragonCommsException e) {
									log.fatal("can't communicate with client: " + e.getMessage());
								}
							}).onRunning((op3) -> {
								node.startTopology(scommand.topologyName);
								((StartTopoGroupOp)op3).receiveSuccess(node.getComms(), node.getComms().getMyNodeDescriptor());
							});
					}, (op2, error) -> {
						try {
							node.getComms().sendServiceMessage(
									new RunTopoErrorSMsg(scommand.topologyName, error), scommand);
						} catch (DragonCommsException e) {
							log.fatal("can't communicate with client: " + e.getMessage());
						}
					}).onRunning((op2) -> {
						try {
							node.prepareTopology(scommand.topologyName, scommand.conf, dragonTopology, false);
							((PrepareTopoGroupOp) op2).receiveSuccess(node.getComms(),
									node.getComms().getMyNodeDescriptor());
						} catch (DragonRequiresClonableException e) {
							((PrepareTopoGroupOp) op2).receiveError(node.getComms(),
									node.getComms().getMyNodeDescriptor(), e.getMessage());
						}
					});
				}, (op, error) -> {
					try {
						node.getComms().sendServiceMessage(
								new RunTopoErrorSMsg(scommand.topologyName, error), scommand);
					} catch (DragonCommsException e) {
						log.fatal("can't communicate with client: " + e.getMessage());
					}
				}).onRunning((op) -> {
					((RunTopoGroupOp) op).receiveSuccess(node.getComms(),
							node.getComms().getMyNodeDescriptor());
				});

		}
	}
	
	/**
	 * Get the list of daemons that this daemon knows about.
	 * @param command
	 */
	private void processGetNodeContext(ServiceMessage command){
		try {
			node.getComms().sendServiceMessage(
					new NodeContextSMsg(node.getNodeProcessor().getContext()),command);
		} catch (DragonCommsException e) {
			log.fatal("can't communicate with client: "+e.getMessage());
		}
	}
	
	/**
	 * Get metrics for this daemon only.
	 * @param command
	 */
	private void processGetMetrics(ServiceMessage command){
		GetMetricsSMsg gm = (GetMetricsSMsg) command;
		if((Boolean)node.getConf().getDragonMetricsEnabled()){
			ComponentMetricMap cm = node.getMetrics(gm.topologyId);
			if(cm!=null){
				try {
					node.getComms().sendServiceMessage(new MetricsSMsg(cm),command);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: "+e.getMessage());
				}
			} else {
				try {
					node.getComms().sendServiceMessage(new GetMetricsErrorSMsg("unknown topology or there are no samples available yet"),command);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: "+e.getMessage());
				}
			}
		} else {
			log.warn("metrics are not enabled");
			try {
				node.getComms().sendServiceMessage(new GetMetricsErrorSMsg("metrics are not enabled in dragon.yaml for this node"),command);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: "+e.getMessage());
			}
		}
	}
	
	/**
	 * Terminate the topology, which consists of:
	 * - terminate the topology on all daemons
	 * - remove the topology from the router on all daemons
	 * @param command contains the name of the topology
	 */
	private void processTerminateTopology(ServiceMessage command){
		TermTopoSMsg tt = (TermTopoSMsg) command;
		if(!node.getLocalClusters().containsKey(tt.topologyId)){
			try {
				node.getComms().sendServiceMessage(new TermTopoErrorSMsg(tt.topologyId,"topology does not exist"),command);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: "+e.getMessage());
			}
		} else {
			final DragonTopology topology = node.getLocalClusters().get(tt.topologyId).getTopology();
			Operations.getInstance().newTermTopoGroupOp(tt,
				(op)->{
					Operations.getInstance().newTermRouterGroupOp(tt,
							topology,
							(op2)->{
								try {
									node.getComms().sendServiceMessage(new TopologyTerminatedMessage(tt.topologyId),tt);
								} catch (DragonCommsException e) {
									log.fatal("can't communicate with client: "+e.getMessage());
								}
							},
							(op2,error)->{
								try {
									node.getComms().sendServiceMessage(new TermTopoErrorSMsg(tt.topologyId,error),tt);
								} catch (DragonCommsException e) {
									log.fatal("can't communicate with client: "+e.getMessage());
								}
							}).onRunning((op2)->{
								node.removeTopo(tt.topologyId);
								((TermRouterGroupOp)op2).receiveSuccess(node.getComms(), node.getComms().getMyNodeDescriptor());
							});
					
				},
				(op,error)->{
					try {
						node.getComms().sendServiceMessage(new TermTopoErrorSMsg(tt.topologyId,error),tt);
					} catch (DragonCommsException e) {
						log.error("could not send terminate topology error message");
					}
				}).onRunning((op)->{
					node.stopTopology(tt.topologyId, (TermTopoGroupOp)op); // starts a thread to stop the topology
					// can't send success for this node until the topology has actually stopped
				});
			
		}
	}
	
	/**
	 * List general information for all topologies running on all daemons.
	 * @param command
	 */
	private void processListTopologies(ServiceMessage command){
		ListToposSMsg ltm = (ListToposSMsg)command;
		Operations.getInstance().newListToposGroupOp(ltm,
			(op)->{
				ListToposGroupOp ltgo = (ListToposGroupOp)op;
					try {
						node.getComms().sendServiceMessage(new TopoListSMsg(ltgo.descState,ltgo.descErrors),command);
					} catch (DragonCommsException e) {
						log.fatal("can't communicate with client: "+e.getMessage());
					}
				},
			(op,error)->{
					log.fatal(error);
		}).onRunning((op)->{
			ListToposGroupOp ltgo = (ListToposGroupOp) op;
			node.listTopologies(ltgo);
			ltgo.aggregate(node.getComms().getMyNodeDescriptor(),
					ltgo.state,ltgo.errors);
		});
	}
	
	/**
	 * Halt the topology. A halted topology can be resumed.
	 * @param command contains the name of the topology to halt.
	 */
	private void processHaltTopology(ServiceMessage command){
		HaltTopoSMsg htm = (HaltTopoSMsg) command;
		if(!node.getLocalClusters().containsKey(htm.topologyId)){
			try {
				node.getComms().sendServiceMessage(new HaltTopoErrorSMsg(htm.topologyId,"topology does not exist"),command);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: "+e.getMessage());
			}
		} else {
			Operations.getInstance().newHaltTopoGroupOp(htm,
				(op)->{
					try {
						node.getComms().sendServiceMessage(new TopoHaltedSMsg(htm.topologyId),command);
					} catch (DragonCommsException e) {
						log.fatal("can't communicate with client: "+e.getMessage());
					}
				},
				(op,error)->{
					try {
						node.getComms().sendServiceMessage(new HaltTopoErrorSMsg(htm.topologyId,error),command);
					} catch (DragonCommsException e) {
						log.fatal("can't communicate with client: "+e.getMessage());
					}
			}).onRunning((op)->{
				node.haltTopology(htm.topologyId);
				((HaltTopoGroupOp)op).receiveSuccess(node.getComms(), node.getComms().getMyNodeDescriptor());
			});
		}
	}
	
	/**
	 * Resume a halted topology.
	 * @param command contains the name of the topology to resume.
	 */
	private void processResumeTopology(ServiceMessage command){
		ResumeTopoSMsg htm = (ResumeTopoSMsg) command;
		if(!node.getLocalClusters().containsKey(htm.topologyId)){
			try {
				node.getComms().sendServiceMessage(new ResumeTopoErrorSMsg(htm.topologyId,"topology does not exist"),command);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: "+e.getMessage());
			}
		} else {
			Operations.getInstance().newResumeTopoGroupOp(htm,
				(op)->{
					try {
						node.getComms().sendServiceMessage(new TopoResumedMsg(htm.topologyId),command);
					} catch (DragonCommsException e) {
						log.fatal("can't communicate with client: "+e.getMessage());
					}
				},
				(op,error)->{
					try {
						node.getComms().sendServiceMessage(new ResumeTopoErrorSMsg(htm.topologyId,error),command);
					} catch (DragonCommsException e) {
						log.fatal("can't communicate with client: "+e.getMessage());
					}
				}).onRunning((op)->{
					node.resumeTopology(htm.topologyId);
					((ResumeTopoGroupOp)op).receiveSuccess(node.getComms(), node.getComms().getMyNodeDescriptor());
				});
			
		}
	}
	
	@Override
	public void run(){
		while(!shouldTerminate){
			ServiceMessage command;
			try {
				command = node.getComms().receiveServiceMessage();
			} catch (InterruptedException e) {
				log.info("interrupted");
				break;
			}
			switch(command.getType()){
			case UPLOAD_JAR:
				processUploadJar(command);
				break;
			case RUN_TOPOLOGY:
				processRunTopology(command);
				break;	
			case GET_NODE_CONTEXT:
				processGetNodeContext(command);
				break;
			case GET_METRICS:
				processGetMetrics(command);
				break;
			case TERMINATE_TOPOLOGY:
				processTerminateTopology(command);
				break;
			case LIST_TOPOLOGIES:
				processListTopologies(command);
				break;
			case HALT_TOPOLOGY:
				processHaltTopology(command);
				break;
			case RESUME_TOPOLOGY:
				processResumeTopology(command);
				break;
			default:
				log.error("unrecognized command: "+command.getType().name());
			}
		}
	}
}
