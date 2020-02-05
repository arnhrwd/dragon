package dragon.network;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.DragonRequiresClonableException;
import dragon.network.Node.NodeState;
import dragon.network.comms.DragonCommsException;
import dragon.network.messages.IErrorMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.allocpart.AllocPartErrorNMsg;
import dragon.network.messages.node.allocpart.AllocPartNMsg;
import dragon.network.messages.node.allocpart.PartAllocedNMsg;
import dragon.network.messages.node.context.ContextUpdateNMsg;
import dragon.network.messages.node.getstatus.GetStatusErrorNMsg;
import dragon.network.messages.node.getstatus.GetStatusNMsg;
import dragon.network.messages.node.getstatus.StatusNMsg;
import dragon.network.messages.node.gettopoinfo.GetTopoInfoNMsg;
import dragon.network.messages.node.gettopoinfo.TopoInfoNMsg;
import dragon.network.messages.node.halttopo.HaltTopoErrorNMsg;
import dragon.network.messages.node.halttopo.HaltTopoNMsg;
import dragon.network.messages.node.halttopo.TopoHaltedNMsg;
import dragon.network.messages.node.preparejar.JarReadyNMsg;
import dragon.network.messages.node.preparejar.PrepareJarErrorNMsg;
import dragon.network.messages.node.preparejar.PrepareJarNMsg;
import dragon.network.messages.node.preparetopo.PrepareTopoNMsg;
import dragon.network.messages.node.preparetopo.TopoReadyNMsg;
import dragon.network.messages.node.removetopo.RemoveTopoErrorNMsg;
import dragon.network.messages.node.removetopo.RemoveTopoNMsg;
import dragon.network.messages.node.removetopo.TopoRemovedNMsg;
import dragon.network.messages.node.resumetopo.ResumeTopoErrorNMsg;
import dragon.network.messages.node.resumetopo.ResumeTopoNMsg;
import dragon.network.messages.node.resumetopo.TopoResumedNMsg;
import dragon.network.messages.node.starttopo.StartTopoNMsg;
import dragon.network.messages.node.starttopo.TopoStartedNMsg;
import dragon.network.messages.node.stoptopo.StopTopoErrorNMsg;
import dragon.network.messages.node.stoptopo.StopTopoNMsg;
import dragon.network.messages.node.stoptopo.TopoStoppedNMsg;
import dragon.network.operations.AllocPartGroupOp;
import dragon.network.operations.GetStatusGroupOp;
import dragon.network.operations.JoinGroupOp;
import dragon.network.operations.ListToposGroupOp;

/**
 * Process node message, which are messages that can only originate from
 * other nodes (daemons). Reads nodes messages from the Comms layer, one
 * at a time.
 * @author aaron
 *
 */
public class NodeMsgProcessor extends Thread {
	private final static Logger log = LogManager.getLogger(NodeMsgProcessor.class);
	
	/**
	 * The node that this node processor belongs to.
	 */
	private final Node node;
	
	/**
	 * The set of node descriptors that this node processor knows about.
	 */
	private final NodeContext context;
	
	/**
	 * 
	 * @param node
	 */
	public NodeMsgProcessor(Node node) {
		this.node=node;
		context=new NodeContext();
		context.put(node.getComms().getMyNodeDesc());
		setName("node processor");
		start();
	}
	
	/**
	 * Process an error message for the group operation.
	 * @param msg is an error message for a group operation
	 */
	private void receiveError(NodeMessage msg) {
		node.getOpsProcessor()
		.getGroupOp(msg.getGroupOp().getId())
		.receiveError(node.getComms(), msg,
				((IErrorMessage)msg).getError());
	}
	
	/**
	 * Process a success message for the group operation.
	 * @param msg is a success message for a group operation
	 */
	private void receiveSuccess(NodeMessage msg) {
		node.getOpsProcessor()
		.getGroupOp(msg.getGroupOp().getId())
		.receiveSuccess(node.getComms(),msg);
	}
	
	/**
	 * Send a success message for a group operation. This should
	 * be sent if the requested operation was successfully completed
	 * on this daemon.
	 * @param msg the node message that requested the group operation,
	 * and that is being replied to
	 */
	private void sendSuccess(NodeMessage msg) {
		msg.getGroupOp().sendSuccess(node.getComms());
	}
	
	/**
	 * Send an error message for a group operation. This should
	 * be sent if the requested operation was not successfully completed
	 * on this daemon. 
	 * @param msg the node message that requested the group operation,
	 * and that is being replied to
	 * @param error an informative message explaining the error
	 */
	private void sendError(NodeMessage msg,String error) {
		msg.getGroupOp().sendError(node.getComms(),error);
	}
	
	
	/*
	 * Processes that require a node state other than OPERATIONAL
	 */
	
	/**
	 * @param msg
	 */
	private synchronized void processAcceptingJoin(NodeMessage msg) {
		if(node.getNodeState()!=NodeState.JOIN_REQUESTED) {
			log.error("unexpected message: "+NodeMessage.NodeMessageType.ACCEPTING_JOIN.name());
		} else {
			receiveSuccess(msg);
		}
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processJoinComplete(NodeMessage msg) {
		if(node.getNodeState()!=NodeState.ACCEPTING_JOIN) {
			log.error("unexpected message: "+NodeMessage.NodeMessageType.JOIN_COMPLETE.name());
		} else {
			node.setNodeState(NodeState.OPERATIONAL);
		}
	}
	
	/*
	 * Processes that require OPERATIONAL node state.
	 */
	
	/**
	 * @param msg
	 */
	private synchronized void processJoinRequest(NodeMessage msg) {
		node.setNodeState(NodeState.ACCEPTING_JOIN);
		context.put(msg.getSender());
		JoinGroupOp jgo = (JoinGroupOp) msg.getGroupOp();
		jgo.context=context;
		sendSuccess(msg);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processContextUpdate(NodeMessage msg) {
		ContextUpdateNMsg cu = (ContextUpdateNMsg) msg;
		boolean hit=false;
		for(String key : context.keySet()) {
			if(!cu.context.containsKey(key)) {
				context.putAll(cu.context);
				try {
					node.getComms().sendNodeMsg(msg.getSender(), new ContextUpdateNMsg(context));
				} catch (DragonCommsException e) {
					log.error("could not send context update to ["+msg.getSender()+"]");
					// TODO: possibly signal that the node has failed
				}
				hit=true;
				break;
			}
		}
		if(!hit) context.putAll(cu.context);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processPrepareJar(NodeMessage msg) {
		PrepareJarNMsg pjf = (PrepareJarNMsg) msg;
		if(!node.storeJarFile(pjf.topologyId,pjf.topologyJar)) {
			sendError(pjf,"could not store the topology jar");
			return;
		} else if(!node.loadJarFile(pjf.topologyId)) {
			sendError(pjf,"could not load the topology jar");
			return;
		}
		sendSuccess(pjf);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processPrepareJarError(NodeMessage msg) {
		PrepareJarErrorNMsg pjem = (PrepareJarErrorNMsg) msg;
		receiveError(pjem);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processJarReady(NodeMessage msg) {
		JarReadyNMsg jrm = (JarReadyNMsg) msg;
		receiveSuccess(jrm);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processPrepareTopology(NodeMessage msg) {
		PrepareTopoNMsg pt = (PrepareTopoNMsg) msg;
		try {
			try {
				node.prepareTopology(pt.topoloyId, pt.conf, pt.topology, false);
			} catch (DragonTopologyException e) {
				sendError(pt,e.getMessage());
			}
			sendSuccess(pt);
		} catch (DragonRequiresClonableException e) {
			sendError(pt,e.getMessage());
		}
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processTopologyReady(NodeMessage msg) {
		TopoReadyNMsg tr = (TopoReadyNMsg) msg;
		receiveSuccess(tr);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processStartTopology(NodeMessage msg) {
		StartTopoNMsg st = (StartTopoNMsg) msg;
		try {
			node.startTopology(st.topologyId);
			sendSuccess(st);
		} catch (DragonTopologyException e) {
			sendError(st,e.getMessage());
		}
		
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processTopologyStarted(NodeMessage msg) {
		TopoStartedNMsg tsm = (TopoStartedNMsg) msg;
		receiveSuccess(tsm);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processStopTopology(NodeMessage msg) {
		StopTopoNMsg stm = (StopTopoNMsg) msg;
		try {
			// starts a thread to stop the topology
			log.debug("asking node to stop the topology ["+stm.topologyId+"]");
			node.terminateTopology(stm.topologyId,stm.getGroupOp());
		} catch (DragonTopologyException e) {
			sendError(stm,e.getMessage());
		} 
	}
		
	/**
	 * @param msg
	 */
	private synchronized void processTopologyStopped(NodeMessage msg) {
		TopoStoppedNMsg tsm = (TopoStoppedNMsg) msg;
		receiveSuccess(tsm);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processStopTopologyError(NodeMessage msg) {
		StopTopoErrorNMsg stem = (StopTopoErrorNMsg) msg;
		receiveError(stem);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processRemoveTopology(NodeMessage msg) {
		RemoveTopoNMsg trm = (RemoveTopoNMsg) msg;
		try {
			node.removeTopo(trm.topologyId);
			sendSuccess(trm);
		} catch (DragonTopologyException e) {
			sendError(trm,e.getMessage());
		}
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processTopologyRemoved(NodeMessage msg) {
		TopoRemovedNMsg rtm = (TopoRemovedNMsg) msg;
		receiveSuccess(rtm);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processRemoveTopologyError(NodeMessage msg) {
		RemoveTopoErrorNMsg trm = (RemoveTopoErrorNMsg) msg;
		receiveError(trm);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processHaltTopology(NodeMessage msg) {
		HaltTopoNMsg htm = (HaltTopoNMsg) msg;
		try {
			node.haltTopology(htm.topologyId);
			sendSuccess(htm);
		} catch (DragonTopologyException e) {
			sendError(htm,e.getMessage());
		}
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processTopologyHalted(NodeMessage msg) {
		TopoHaltedNMsg thm = (TopoHaltedNMsg) msg;
		receiveSuccess(thm);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processHaltTopologyError(NodeMessage msg) {
		HaltTopoErrorNMsg htem = (HaltTopoErrorNMsg) msg;
		receiveError(htem);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processResumeTopology(NodeMessage msg) {
		ResumeTopoNMsg htm = (ResumeTopoNMsg) msg;
		try {
			node.resumeTopology(htm.topologyId);
			sendSuccess(htm);
		} catch (DragonTopologyException e) {
			sendError(htm,e.getMessage());
		}
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processTopologyResumed(NodeMessage msg) {
		TopoResumedNMsg thm = (TopoResumedNMsg) msg;
		receiveSuccess(thm);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processResumeTopologyError(NodeMessage msg) {
		ResumeTopoErrorNMsg htem = (ResumeTopoErrorNMsg) msg;
		receiveError(htem);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processGetTopologyInformation(NodeMessage msg) {
		GetTopoInfoNMsg gtim = (GetTopoInfoNMsg) msg;
		ListToposGroupOp ltgo = (ListToposGroupOp)gtim.getGroupOp();
		node.listTopologies(ltgo); 
		ltgo.sendSuccess(node.getComms());
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processTopologyInformation(NodeMessage msg) {
		TopoInfoNMsg tim = (TopoInfoNMsg) msg;
		((ListToposGroupOp)(node.getOpsProcessor()
				.getGroupOp(tim.getGroupOp().getId())))
				.aggregate(tim.getSender(),tim.state,tim.errors);
		receiveSuccess(tim);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processAllocatePartition(NodeMessage msg) {
		AllocPartNMsg apm = (AllocPartNMsg) msg;
		AllocPartGroupOp apgo = (AllocPartGroupOp) apm.getGroupOp();
		int a=node.allocatePartition(apm.partitionId, apm.number);
		apgo.partitionId=apm.partitionId;
		apgo.daemons=a;
		if(a==apm.number) {
			apgo.sendSuccess(node.getComms());
		} else {
			apgo.sendError(node.getComms(), "failed to start daemon processes on ["+msg.getSender()+"]");
		}
	}
	
	/**
	 * 
	 * @param msg
	 */
	private synchronized void processPartitionAllocated(NodeMessage msg) {
		PartAllocedNMsg pam = (PartAllocedNMsg) msg;
		receiveSuccess(pam);
	}
	
	/**
	 * 
	 * @param msg
	 */
	private synchronized void processAllocatePartitionError(NodeMessage msg) {
		AllocPartErrorNMsg apem = (AllocPartErrorNMsg) msg;
		receiveError(apem);
	}
	
	/**
	 * 
	 * @param msg
	 */
	private synchronized void processGetStatus(NodeMessage msg) {
		GetStatusNMsg gsnm = (GetStatusNMsg) msg;
		GetStatusGroupOp gsgo = (GetStatusGroupOp) gsnm.getGroupOp();
		NodeStatus nodeStatus = node.getStatus();
		nodeStatus.context = context;
		gsgo.nodeStatus=nodeStatus;
		gsgo.sendSuccess(node.getComms());
	}
	
	/**
	 * 
	 * @param msg
	 */
	private synchronized void processGetStatusError(NodeMessage msg) {
		GetStatusErrorNMsg gsnm = (GetStatusErrorNMsg) msg;
		receiveError(gsnm);
	}
	
	/**
	 * 
	 * @param msg
	 */
	private synchronized void processStatus(NodeMessage msg) {
		StatusNMsg gsnm = (StatusNMsg) msg;
		((GetStatusGroupOp)(node.getOpsProcessor()
				.getGroupOp(gsnm.getGroupOp().getId())))
				.aggregate(gsnm.nodeStatus);
		receiveSuccess(msg);
	}
	
	/**
	 * @param msg
	 */
	private void processOperationalMsgs(NodeMessage msg) {
		switch(msg.getType()) {
		case JOIN_REQUEST:
			processJoinRequest(msg);
			break;
		case CONTEXT_UPDATE:
			processContextUpdate(msg);
			break;
		case PREPARE_JAR:
			processPrepareJar(msg);
			break;
		case PREPARE_JAR_ERROR:
			processPrepareJarError(msg);
			break;
		case JAR_READY:
			processJarReady(msg);
			break;
		case PREPARE_TOPOLOGY:
			processPrepareTopology(msg);
			break;
		case TOPOLOGY_READY:
			processTopologyReady(msg);
			break;
		case START_TOPOLOGY:
			processStartTopology(msg);
			break;
		case TOPOLOGY_STARTED:
			processTopologyStarted(msg);
			break;
		case STOP_TOPOLOGY:
			processStopTopology(msg);
			break;
		case TOPOLOGY_STOPPED:
			processTopologyStopped(msg);
			break;
		case STOP_TOPOLOGY_ERROR:
			processStopTopologyError(msg);
			break;
		case REMOVE_TOPOLOGY:
			processRemoveTopology(msg);
			break;
		case TOPOLOGY_REMOVED:
			processTopologyRemoved(msg);
			break;
		case REMOVE_TOPOLOGY_ERROR:
			processRemoveTopologyError(msg);
			break;
		case HALT_TOPOLOGY:
			processHaltTopology(msg);
			break;
		case TOPOLOGY_HALTED:
			processTopologyHalted(msg);
			break;
		case HALT_TOPOLOGY_ERROR:
			processHaltTopologyError(msg);
			break;
		case RESUME_TOPOLOGY:
			processResumeTopology(msg);
			break;
		case TOPOLOGY_RESUMED:
			processTopologyResumed(msg);
			break;
		case RESUME_TOPOLOGY_ERROR:
			processResumeTopologyError(msg);
			break;
		case GET_TOPOLOGY_INFORMATION:
			processGetTopologyInformation(msg);
			break;
		case TOPOLOGY_INFORMATION:
			processTopologyInformation(msg);
			break;
		case ALLOCATE_PARTITION:
			processAllocatePartition(msg);
			break;
		case ALLOCATE_PARTITION_ERROR:
			processAllocatePartitionError(msg);
			break;
		case PARTITION_ALLOCATED:
			processPartitionAllocated(msg);
			break;
		case GET_STATUS:
			processGetStatus(msg);
			break;
		case GET_STATUS_ERROR:
			processGetStatusError(msg);
			break;
		case STATUS:
			processStatus(msg);
			break;
		default:
			break;
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		log.info("starting up");
		while(!isInterrupted()) {
			NodeMessage msg;
			try {
				msg = node.getComms().receiveNodeMsg();
			} catch (InterruptedException e) {
				log.info("interrupted");
				break;
			}
			log.debug("received ["+msg.getType().name()+"] from ["+msg.getSender());
			switch(msg.getType()) {
			case ACCEPTING_JOIN:
				processAcceptingJoin(msg);
				break;
			case JOIN_COMPLETE:
				processJoinComplete(msg);
				break;
			case CONTEXT_UPDATE:
			case GET_TOPOLOGY_INFORMATION:
			case HALT_TOPOLOGY:
			case HALT_TOPOLOGY_ERROR:
			case JAR_READY:
			case JOIN_REQUEST:
			case PREPARE_JAR:
			case PREPARE_JAR_ERROR:
			case PREPARE_TOPOLOGY:
			case PREPARE_TOPOLOGY_ERROR:
			case REMOVE_TOPOLOGY:
			case REMOVE_TOPOLOGY_ERROR:
			case RESUME_TOPOLOGY:
			case RESUME_TOPOLOGY_ERROR:
			case START_TOPOLOGY:
			case START_TOPOLOGY_ERROR:
			case STOP_TOPOLOGY:
			case STOP_TOPOLOGY_ERROR:
			case TOPOLOGY_HALTED:
			case TOPOLOGY_INFORMATION:
			case TOPOLOGY_READY:
			case TOPOLOGY_REMOVED:
			case TOPOLOGY_RESUMED:
			case TOPOLOGY_STARTED:
			case TOPOLOGY_STOPPED:
			case ALLOCATE_PARTITION:
			case ALLOCATE_PARTITION_ERROR:
			case PARTITION_ALLOCATED:
			case GET_STATUS:
			case GET_STATUS_ERROR:
			case STATUS:
				// do when appropriate
				node.getOpsProcessor().newConditionOp((op)->{
					return node.getNodeState()==NodeState.OPERATIONAL;
				},(op)->{
					try {
						node.getOperationsLock().lockInterruptibly();
						processOperationalMsgs(msg);
					} catch (InterruptedException e) {
						log.error("interrupted while waiting for node operations lock");
					} finally {
						node.getOperationsLock().unlock();
					}
				}, (op,error)->{
					log.error(error);
				});
				break;
			default:
				break;
			}
		}
		log.info("shutting down");
	}
	
	/**
	 * 
	 * @return the node context
	 */
	public NodeContext getContext() {
		return context;
	}
	
	/**
	 * Put all of the given context into the node's context.
	 * @param context
	 */
	public synchronized void contextPutAll(NodeContext context) {
		this.context.putAll(context);
	}
}
