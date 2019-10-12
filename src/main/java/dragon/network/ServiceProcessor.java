package dragon.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.DragonRequiresClonableException;
import dragon.metrics.ComponentMetricMap;
import dragon.network.messages.service.RunTopologyMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TerminateTopologyErrorMessage;
import dragon.network.messages.service.TerminateTopologyMessage;
import dragon.network.messages.service.TopologyHaltedMessage;
import dragon.network.messages.service.TopologyListMessage;
import dragon.network.messages.service.TopologyResumedMessage;
import dragon.network.messages.service.TopologyRunningMessage;
import dragon.network.messages.service.TopologyTerminatedMessage;
import dragon.network.messages.service.UploadJarFailedMessage;
import dragon.network.messages.service.RunTopologyErrorMessage;
import dragon.network.comms.DragonCommsException;
import dragon.network.messages.service.GetMetricsMessage;
import dragon.network.messages.service.HaltTopologyErrorMessage;
import dragon.network.messages.service.HaltTopologyMessage;
import dragon.network.messages.service.ListTopologiesMessage;
import dragon.network.messages.service.UploadJarMessage;
import dragon.network.messages.service.UploadJarSuccessMessage;
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
import dragon.network.messages.service.GetMetricsErrorMessage;
import dragon.network.messages.service.MetricsMessage;
import dragon.network.messages.service.NodeContextMessage;
import dragon.network.messages.service.ResumeTopologyErrorMessage;
import dragon.network.messages.service.ResumeTopologyMessage;

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
		UploadJarMessage jf = (UploadJarMessage) command;
		if(node.getLocalClusters().containsKey(jf.topologyName)){
			try {
				node.getComms().sendServiceMessage(new UploadJarFailedMessage(jf.topologyName,"topology exists"),jf);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: "+e.getMessage());
			}
		} else {
			log.info("storing topology ["+jf.topologyName+"]");
			if(!node.storeJarFile(jf.topologyName,jf.topologyJar)) {
				try {
					node.getComms().sendServiceMessage(new UploadJarFailedMessage(jf.topologyName,"could not store the topology jar"),jf);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: "+e.getMessage());
				}
				return;
			}
			if(!node.loadJarFile(jf.topologyName)) {
				try {
					node.getComms().sendServiceMessage(new UploadJarFailedMessage(jf.topologyName,"could not load the topology jar"),jf);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: "+e.getMessage());
				}				
				return;
			}
			try {
				node.getComms().sendServiceMessage(new UploadJarSuccessMessage(jf.topologyName),jf);
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
		RunTopologyMessage scommand = (RunTopologyMessage) command;
		if(node.getLocalClusters().containsKey(scommand.topologyName)){
			try {
				node.getComms().sendServiceMessage(new RunTopologyErrorMessage(scommand.topologyName,"topology exists"),scommand);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: "+e.getMessage());
			}
		} else {
			final DragonTopology dragonTopology = scommand.dragonTopology;
			Operations.getInstance().newRunTopoGroupOp(scommand,
				node.readJarFile(scommand.topologyName), dragonTopology, (op) -> {
					scommand.dragonTopology = dragonTopology; // undo a side-effect from the run topology operation
					Operations.getInstance().newPrepareTopoGroupOp(scommand, (op2) -> {
						Operations.getInstance()
							.newStartTopologyGroupOperation(scommand, (op3) -> {
								try {
									node.getComms().sendServiceMessage(
											new TopologyRunningMessage(scommand.topologyName), scommand);
								} catch (DragonCommsException e) {
									log.fatal("can't communicate with client: " + e.getMessage());
								}
							}, (op3, error) -> {
								try {
									node.getComms().sendServiceMessage(
											new RunTopologyErrorMessage(scommand.topologyName, error),
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
									new RunTopologyErrorMessage(scommand.topologyName, error), scommand);
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
								new RunTopologyErrorMessage(scommand.topologyName, error), scommand);
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
					new NodeContextMessage(node.getNodeProcessor().getContext()),command);
		} catch (DragonCommsException e) {
			log.fatal("can't communicate with client: "+e.getMessage());
		}
	}
	
	/**
	 * Get metrics for this daemon only.
	 * @param command
	 */
	private void processGetMetrics(ServiceMessage command){
		GetMetricsMessage gm = (GetMetricsMessage) command;
		if((Boolean)node.getConf().getDragonMetricsEnabled()){
			ComponentMetricMap cm = node.getMetrics(gm.topologyId);
			if(cm!=null){
				try {
					node.getComms().sendServiceMessage(new MetricsMessage(cm),command);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: "+e.getMessage());
				}
			} else {
				try {
					node.getComms().sendServiceMessage(new GetMetricsErrorMessage("unknown topology or there are no samples available yet"),command);
				} catch (DragonCommsException e) {
					log.fatal("can't communicate with client: "+e.getMessage());
				}
			}
		} else {
			log.warn("metrics are not enabled");
			try {
				node.getComms().sendServiceMessage(new GetMetricsErrorMessage("metrics are not enabled in dragon.yaml for this node"),command);
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
		TerminateTopologyMessage tt = (TerminateTopologyMessage) command;
		if(!node.getLocalClusters().containsKey(tt.topologyId)){
			try {
				node.getComms().sendServiceMessage(new TerminateTopologyErrorMessage(tt.topologyId,"topology does not exist"),command);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: "+e.getMessage());
			}
		} else {
			final DragonTopology topology = node.getLocalClusters().get(tt.topologyId).getTopology();
			Operations.getInstance().newTermTopoGroupOp(tt,
				(op)->{
					Operations.getInstance().newTermRouterGroupOp(tt,
							(op2)->{
								try {
									node.getComms().sendServiceMessage(new TopologyTerminatedMessage(tt.topologyId),tt);
								} catch (DragonCommsException e) {
									log.fatal("can't communicate with client: "+e.getMessage());
								}
							},
							(op2,error)->{
								try {
									node.getComms().sendServiceMessage(new TerminateTopologyErrorMessage(tt.topologyId,error),tt);
								} catch (DragonCommsException e) {
									log.fatal("can't communicate with client: "+e.getMessage());
								}
							}).onRunning((op2)->{
								node.getRouter().terminateTopology(tt.topologyId,topology);
								((TermRouterGroupOp)op2).receiveSuccess(node.getComms(), node.getComms().getMyNodeDescriptor());
							});
					
				},
				(op,error)->{
					try {
						node.getComms().sendServiceMessage(new TerminateTopologyErrorMessage(tt.topologyId,error),tt);
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
		ListTopologiesMessage ltm = (ListTopologiesMessage)command;
		Operations.getInstance().newListToposGroupOp(ltm,
			(op)->{
				ListToposGroupOp ltgo = (ListToposGroupOp)op;
					try {
						node.getComms().sendServiceMessage(new TopologyListMessage(ltgo.descState,ltgo.descErrors),command);
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
		HaltTopologyMessage htm = (HaltTopologyMessage) command;
		if(!node.getLocalClusters().containsKey(htm.topologyId)){
			try {
				node.getComms().sendServiceMessage(new HaltTopologyErrorMessage(htm.topologyId,"topology does not exist"),command);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: "+e.getMessage());
			}
		} else {
			Operations.getInstance().newHaltTopoGroupOp(htm,
				(op)->{
					try {
						node.getComms().sendServiceMessage(new TopologyHaltedMessage(htm.topologyId),command);
					} catch (DragonCommsException e) {
						log.fatal("can't communicate with client: "+e.getMessage());
					}
				},
				(op,error)->{
					try {
						node.getComms().sendServiceMessage(new HaltTopologyErrorMessage(htm.topologyId,error),command);
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
		ResumeTopologyMessage htm = (ResumeTopologyMessage) command;
		if(!node.getLocalClusters().containsKey(htm.topologyId)){
			try {
				node.getComms().sendServiceMessage(new ResumeTopologyErrorMessage(htm.topologyId,"topology does not exist"),command);
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: "+e.getMessage());
			}
		} else {
			Operations.getInstance().newResumeTopoGroupOp(htm,
				(op)->{
					try {
						node.getComms().sendServiceMessage(new TopologyResumedMessage(htm.topologyId),command);
					} catch (DragonCommsException e) {
						log.fatal("can't communicate with client: "+e.getMessage());
					}
				},
				(op,error)->{
					try {
						node.getComms().sendServiceMessage(new ResumeTopologyErrorMessage(htm.topologyId,error),command);
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
