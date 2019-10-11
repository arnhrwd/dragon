package dragon.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.DragonRequiresClonableException;
import dragon.metrics.ComponentMetricMap;
import dragon.network.messages.service.RunTopologyMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TerminateTopologyErrorMessage;
import dragon.network.messages.service.TerminateTopologyMessage;
import dragon.network.messages.service.TopologyRunningMessage;
import dragon.network.messages.service.TopologyTerminatedMessage;
import dragon.network.messages.service.UploadJarFailedMessage;
import dragon.network.messages.service.RunTopologyErrorMessage;
import dragon.network.comms.DragonCommsException;
import dragon.network.messages.service.GetMetricsMessage;
import dragon.network.messages.service.HaltTopologyErrorMessage;
import dragon.network.messages.service.HaltTopologyMessage;
import dragon.network.messages.service.UploadJarMessage;
import dragon.network.messages.service.UploadJarSuccessMessage;
import dragon.network.operations.HaltTopologyGroupOperation;
import dragon.network.operations.ListTopologiesGroupOperation;
import dragon.network.operations.PrepareTopologyGroupOperation;
import dragon.network.operations.ResumeTopologyGroupOperation;
import dragon.network.operations.RunTopologyGroupOperation;
import dragon.network.operations.StartTopologyGroupOperation;
import dragon.network.operations.TerminateRouterGroupOperation;
import dragon.network.operations.TerminateTopologyGroupOperation;
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
			final DragonTopology dragonTopology=scommand.dragonTopology;
			RunTopologyGroupOperation rtgc = (RunTopologyGroupOperation)
				node.newGroupOperation(new RunTopologyGroupOperation(scommand,
				node.readJarFile(scommand.topologyName),
				()->{
					scommand.dragonTopology=dragonTopology; // undo a side-effect from the run topology operation
					PrepareTopologyGroupOperation ptgo = (PrepareTopologyGroupOperation)
						node.newGroupOperation(
						new PrepareTopologyGroupOperation(scommand,
						()->{
							StartTopologyGroupOperation stgo = (StartTopologyGroupOperation) 
								node.newGroupOperation(new StartTopologyGroupOperation(scommand,
									()->{
										try {
											node.getComms().sendServiceMessage(new TopologyRunningMessage(scommand.topologyName),scommand);
										} catch (DragonCommsException e) {
											log.fatal("can't communicate with client: "+e.getMessage());
										}
									},
									(error)->{
										try {
											node.getComms().sendServiceMessage(new RunTopologyErrorMessage(scommand.topologyName,error),scommand);
										} catch (DragonCommsException e) {
											log.fatal("can't communicate with client: "+e.getMessage());
										}
									}),
								scommand.topologyName);
							node.startTopology(scommand.topologyName);
							stgo.receiveSuccess(node.getComms(), node.getComms().getMyNodeDescriptor());
						},
						(error)->{
							try {
								node.getComms().sendServiceMessage(new RunTopologyErrorMessage(scommand.topologyName,error),scommand);
							} catch (DragonCommsException e) {
								log.fatal("can't communicate with client: "+e.getMessage());
							}
						}),dragonTopology);
					try {
						node.prepareTopology(scommand.topologyName, scommand.conf, dragonTopology, false);
						ptgo.receiveSuccess(node.getComms(),node.getComms().getMyNodeDescriptor());
					} catch (DragonRequiresClonableException e) {
						ptgo.receiveError(node.getComms(), node.getComms().getMyNodeDescriptor(), e.getMessage());
					}
				},
				(error)->{
					try {
						node.getComms().sendServiceMessage(new RunTopologyErrorMessage(scommand.topologyName,error),scommand);
					} catch (DragonCommsException e) {
						log.fatal("can't communicate with client: "+e.getMessage());
					}
				}),dragonTopology);
			rtgc.receiveSuccess(node.getComms(), node.getComms().getMyNodeDescriptor());
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
			TerminateTopologyGroupOperation ttgo = (TerminateTopologyGroupOperation)
				node.newGroupOperation(new TerminateTopologyGroupOperation(tt,
				()->{
					TerminateRouterGroupOperation trgo = (TerminateRouterGroupOperation)
						node.newGroupOperation(new TerminateRouterGroupOperation(tt,tt.topologyId,
							()->{
								try {
									node.getComms().sendServiceMessage(new TopologyTerminatedMessage(tt.topologyId),tt);
								} catch (DragonCommsException e) {
									log.fatal("can't communicate with client: "+e.getMessage());
								}
							},
							(error)->{
								try {
									node.getComms().sendServiceMessage(new TerminateTopologyErrorMessage(tt.topologyId,error),tt);
								} catch (DragonCommsException e) {
									log.fatal("can't communicate with client: "+e.getMessage());
								}
							}),topology);
					node.getRouter().terminateTopology(tt.topologyId,topology);
					trgo.receiveSuccess(node.getComms(), node.getComms().getMyNodeDescriptor());
				},
				(error)->{
					try {
						node.getComms().sendServiceMessage(new TerminateTopologyErrorMessage(tt.topologyId,error),tt);
					} catch (DragonCommsException e) {
						log.error("could not send terminate topology error message");
					}
				}),
				topology);
			node.stopTopology(tt.topologyId, ttgo); // starts a thread to stop the topology
			// can't send success for this node until the topology has actually stopped
		}
	}
	
	/**
	 * List general information for all topologies running on all daemons.
	 * @param command
	 */
	private void processListTopologies(ServiceMessage command){
		ListTopologiesGroupOperation ltgo = new ListTopologiesGroupOperation(command);
		for(NodeDescriptor desc : node.getNodeProcessor().getContext().values()) {
			ltgo.add(desc);
		}
		node.register(ltgo);
		node.listTopologies(ltgo);
		ltgo.initiate(node.getComms());
		ltgo.aggregate(node.getComms().getMyNodeDescriptor(),
				ltgo.state,ltgo.errors);
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
			HaltTopologyGroupOperation htgo = (HaltTopologyGroupOperation)
					node.newGroupOperation(new HaltTopologyGroupOperation(htm),htm.topologyId);
			node.haltTopology(htm.topologyId);
			htgo.receiveSuccess(node.getComms(), node.getComms().getMyNodeDescriptor());
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
			ResumeTopologyGroupOperation htgo = (ResumeTopologyGroupOperation)
					node.newGroupOperation(new ResumeTopologyGroupOperation(htm),htm.topologyId);
			node.resumeTopology(htm.topologyId);
			htgo.receiveSuccess(node.getComms(), node.getComms().getMyNodeDescriptor());
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
