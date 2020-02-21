package dragon.network;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.DragonInvalidStateException;
import dragon.DragonRequiresClonableException;
import dragon.network.Node.NodeState;
import dragon.network.comms.DragonCommsException;
import dragon.network.messages.IErrorMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.allocpart.AllocPartErrorNMsg;
import dragon.network.messages.node.allocpart.AllocPartNMsg;
import dragon.network.messages.node.allocpart.PartAllocedNMsg;
import dragon.network.messages.node.context.ContextUpdateNMsg;
import dragon.network.messages.node.deallocpart.DeallocPartErrorNMsg;
import dragon.network.messages.node.deallocpart.DeallocPartNMsg;
import dragon.network.messages.node.deallocpart.PartDeallocedNMsg;
import dragon.network.messages.node.fault.NodeFaultNMsg;
import dragon.network.messages.node.fault.TopoFaultNMsg;
import dragon.network.messages.node.getstatus.GetStatusErrorNMsg;
import dragon.network.messages.node.getstatus.GetStatusNMsg;
import dragon.network.messages.node.getstatus.StatusNMsg;
import dragon.network.messages.node.gettopoinfo.GetTopoInfoNMsg;
import dragon.network.messages.node.gettopoinfo.TopoInfoNMsg;
import dragon.network.messages.node.halttopo.HaltTopoErrorNMsg;
import dragon.network.messages.node.halttopo.HaltTopoNMsg;
import dragon.network.messages.node.halttopo.TopoHaltedNMsg;
import dragon.network.messages.node.join.AcceptingJoinNMsg;
import dragon.network.messages.node.join.JoinCompleteNMsg;
import dragon.network.messages.node.join.JoinRequestNMsg;
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
import dragon.network.messages.node.stoptopo.TermTopoErrorNMsg;
import dragon.network.messages.node.stoptopo.TermTopoNMsg;
import dragon.network.messages.node.stoptopo.TopoTermdNMsg;
import dragon.network.messages.node.term.TermNodeNMsg;
import dragon.network.operations.AllocPartGroupOp;
import dragon.network.operations.DeallocPartGroupOp;
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
	public NodeMsgProcessor() {
		this.node=Node.inst();
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
		.receiveError(msg,((IErrorMessage)msg).getError());
	}
	
	/**
	 * Process a success message for the group operation.
	 * @param msg is a success message for a group operation
	 */
	private void receiveSuccess(NodeMessage msg) {
		node.getOpsProcessor()
		.getGroupOp(msg.getGroupOp().getId())
		.receiveSuccess(msg);
	}
	
	/**
	 * Send a success message for a group operation. This should
	 * be sent if the requested operation was successfully completed
	 * on this daemon.
	 * @param msg the node message that requested the group operation,
	 * and that is being replied to
	 */
	private void sendSuccess(NodeMessage msg) {
		msg.getGroupOp().sendSuccess();
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
		msg.getGroupOp().sendError(error);
	}
	
	
	/*
	 * Processes that require a node state other than OPERATIONAL
	 */
	
	/**
	 * @param msg
	 */
	private synchronized void processAcceptingJoin(AcceptingJoinNMsg msg) {
		if(node.getNodeState()!=NodeState.JOIN_REQUESTED) {
			log.error("unexpected message: "+NodeMessage.NodeMessageType.ACCEPTING_JOIN.name());
		} else {
			receiveSuccess(msg);
		}
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processJoinComplete(JoinCompleteNMsg msg) {
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
	private synchronized void processJoinRequest(JoinRequestNMsg msg) {
		node.setNodeState(NodeState.ACCEPTING_JOIN);
		context.put(msg.getSender());
		JoinGroupOp jgo = (JoinGroupOp) msg.getGroupOp();
		jgo.context=context;
		sendSuccess(msg);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processContextUpdate(ContextUpdateNMsg cu) {
		boolean hit=false;
		for(String key : context.keySet()) {
			if(!cu.context.containsKey(key)) {
				context.putAll(cu.context);
				try {
					node.getComms().sendNodeMsg(cu.getSender(), new ContextUpdateNMsg(context));
				} catch (DragonCommsException e) {
					node.nodeFault(cu.getSender());
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
	private synchronized void processPrepareJar(PrepareJarNMsg pjf) {
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
	private synchronized void processPrepareJarError(PrepareJarErrorNMsg pjem) {
		receiveError(pjem);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processJarReady(JarReadyNMsg jrm) {
		receiveSuccess(jrm);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processPrepareTopology(PrepareTopoNMsg pt) {
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
	private synchronized void processTopologyReady(TopoReadyNMsg tr) {
		receiveSuccess(tr);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processStartTopology(StartTopoNMsg st) {
		try {
			node.startTopology(st.topologyId);
			sendSuccess(st);
		} catch (DragonTopologyException | DragonInvalidStateException e) {
			sendError(st,e.getMessage());
		}
		
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processTopologyStarted(TopoStartedNMsg tsm) {
		receiveSuccess(tsm);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processStopTopology(TermTopoNMsg stm) {
		try {
			// starts a thread to stop the topology
			log.debug("asking node to stop the topology ["+stm.topologyId+"]");
			node.terminateTopology(stm.topologyId,stm.getGroupOp());
		} catch (DragonTopologyException | DragonInvalidStateException e) {
			sendError(stm,e.getMessage());
		} 
	}
		
	/**
	 * @param msg
	 */
	private synchronized void processTopologyStopped(TopoTermdNMsg tsm) {
		receiveSuccess(tsm);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processStopTopologyError(TermTopoErrorNMsg stem) {
		receiveError(stem);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processRemoveTopology(RemoveTopoNMsg trm) {
		try {
			node.removeTopo(trm.topologyId,trm.purge);
			sendSuccess(trm);
		} catch (DragonTopologyException e) {
			sendError(trm,e.getMessage());
		}
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processTopologyRemoved(TopoRemovedNMsg rtm) {
		receiveSuccess(rtm);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processRemoveTopologyError(RemoveTopoErrorNMsg trm) {
		receiveError(trm);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processHaltTopology(HaltTopoNMsg htm) {
		try {
			node.haltTopology(htm.topologyId);
			sendSuccess(htm);
		} catch (DragonTopologyException | DragonInvalidStateException e) {
			sendError(htm,e.getMessage());
		}
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processTopologyHalted(TopoHaltedNMsg thm) {
		receiveSuccess(thm);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processHaltTopologyError(HaltTopoErrorNMsg htem) {
		receiveError(htem);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processResumeTopology(ResumeTopoNMsg htm) {
		try {
			node.resumeTopology(htm.topologyId);
			sendSuccess(htm);
		} catch (DragonTopologyException | DragonInvalidStateException e) {
			sendError(htm,e.getMessage());
		}
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processTopologyResumed(TopoResumedNMsg thm) {
		receiveSuccess(thm);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processResumeTopologyError(ResumeTopoErrorNMsg htem) {
		receiveError(htem);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processGetTopologyInformation(GetTopoInfoNMsg gtim) {
		ListToposGroupOp ltgo = (ListToposGroupOp)gtim.getGroupOp();
		node.listTopologies(ltgo); 
		ltgo.sendSuccess();
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processTopologyInformation(TopoInfoNMsg tim) {
		((ListToposGroupOp)(node.getOpsProcessor()
				.getGroupOp(tim.getGroupOp().getId())))
				.aggregate(tim.getSender(),tim.state,tim.errors,tim.components,tim.metrics);
		receiveSuccess(tim);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processAllocatePartition(AllocPartNMsg apm) {
		AllocPartGroupOp apgo = (AllocPartGroupOp) apm.getGroupOp();
		int a=node.allocatePartition(apm.partitionId, apm.number);
		apgo.partitionId=apm.partitionId;
		apgo.number=a;
		if(a==apm.number) {
			apgo.sendSuccess();
		} else {
			apgo.sendError("failed to allocate partitions on ["+apm.getSender()+"]");
		}
	}
	
	/**
	 * 
	 * @param msg
	 */
	private synchronized void processPartitionAllocated(PartAllocedNMsg pam) {
		receiveSuccess(pam);
	}
	
	/**
	 * 
	 * @param msg
	 */
	private synchronized void processAllocatePartitionError(AllocPartErrorNMsg apem) {
		receiveError(apem);
	}
	
	/**
	 * 
	 * @param msg
	 */
	private synchronized void processGetStatus(GetStatusNMsg gsnm) {
		GetStatusGroupOp gsgo = (GetStatusGroupOp) gsnm.getGroupOp();
		NodeStatus nodeStatus = node.getStatus();
		nodeStatus.context = context;
		gsgo.nodeStatus=nodeStatus;
		gsgo.sendSuccess();
	}
	
	/**
	 * 
	 * @param msg
	 */
	private synchronized void processGetStatusError(GetStatusErrorNMsg gsnm) {
		receiveError(gsnm);
	}
	
	/**
	 * 
	 * @param msg
	 */
	private synchronized void processStatus(StatusNMsg gsnm) {
		((GetStatusGroupOp)(node.getOpsProcessor()
				.getGroupOp(gsnm.getGroupOp().getId())))
				.aggregate(gsnm.nodeStatus);
		receiveSuccess(gsnm);
	}
	
	/**
	 * @param msg
	 */
	private synchronized void processDeallocatePartition(DeallocPartNMsg apm) {
		DeallocPartGroupOp apgo = (DeallocPartGroupOp) apm.getGroupOp();
		int a=node.deallocatePartition(apm.partitionId, apm.number);
		apgo.partitionId=apm.partitionId;
		apgo.number=a;
		if(a==apm.number) {
			apgo.sendSuccess();
		} else {
			apgo.sendError("failed to deallocate partitions on ["+apm.getSender()+"]");
		}
	}
	
	/**
	 * 
	 * @param msg
	 */
	private synchronized void processPartitionDeallocated(PartDeallocedNMsg pam) {
		receiveSuccess(pam);
	}
	
	/**
	 * 
	 * @param msg
	 */
	private synchronized void processDeallocatePartitionError(DeallocPartErrorNMsg apem) {
		receiveError(apem);
	}
	
	/**
	 * 
	 * @param msg
	 */
	private synchronized void processTerminateNode(TermNodeNMsg msg) {
		node.terminate();
	}
	
	/**
	 * 
	 * @param msg
	 */
	private synchronized void processNodeFault(NodeFaultNMsg msg) {
		node.removeNode(msg.desc);
	}
	
	/**
	 * 
	 * @param msg
	 */
	private synchronized void processTopologyFault(TopoFaultNMsg msg) {
		try {
			node.setTopologyFault(msg.topologyId);
		} catch (DragonTopologyException e) {
			log.error(e.getMessage());
		}
	}
	
	
	/**
	 * @param msg
	 */
	private void processOperationalMsgs(NodeMessage msg) {
		switch(msg.getType()) {
		case JOIN_REQUEST:
			processJoinRequest((JoinRequestNMsg)msg);
			break;
		case CONTEXT_UPDATE:
			processContextUpdate((ContextUpdateNMsg) msg);
			break;
		case PREPARE_JAR:
			processPrepareJar((PrepareJarNMsg) msg);
			break;
		case PREPARE_JAR_ERROR:
			processPrepareJarError((PrepareJarErrorNMsg) msg);
			break;
		case JAR_READY:
			processJarReady((JarReadyNMsg) msg);
			break;
		case PREPARE_TOPOLOGY:
			processPrepareTopology((PrepareTopoNMsg) msg);
			break;
		case TOPOLOGY_READY:
			processTopologyReady((TopoReadyNMsg) msg);
			break;
		case START_TOPOLOGY:
			processStartTopology((StartTopoNMsg) msg);
			break;
		case TOPOLOGY_STARTED:
			processTopologyStarted((TopoStartedNMsg) msg);
			break;
		case TERMINATE_TOPOLOGY:
			processStopTopology((TermTopoNMsg) msg);
			break;
		case TOPOLOGY_TERMINATED:
			processTopologyStopped((TopoTermdNMsg) msg);
			break;
		case TERMINATE_TOPOLOGY_ERROR:
			processStopTopologyError((TermTopoErrorNMsg) msg);
			break;
		case REMOVE_TOPOLOGY:
			processRemoveTopology((RemoveTopoNMsg) msg);
			break;
		case TOPOLOGY_REMOVED:
			processTopologyRemoved((TopoRemovedNMsg) msg);
			break;
		case REMOVE_TOPOLOGY_ERROR:
			processRemoveTopologyError((RemoveTopoErrorNMsg) msg);
			break;
		case HALT_TOPOLOGY:
			processHaltTopology((HaltTopoNMsg) msg);
			break;
		case TOPOLOGY_HALTED:
			processTopologyHalted((TopoHaltedNMsg) msg);
			break;
		case HALT_TOPOLOGY_ERROR:
			processHaltTopologyError((HaltTopoErrorNMsg) msg);
			break;
		case RESUME_TOPOLOGY:
			processResumeTopology((ResumeTopoNMsg) msg);
			break;
		case TOPOLOGY_RESUMED:
			processTopologyResumed((TopoResumedNMsg) msg);
			break;
		case RESUME_TOPOLOGY_ERROR:
			processResumeTopologyError((ResumeTopoErrorNMsg) msg);
			break;
		case GET_TOPOLOGY_INFORMATION:
			processGetTopologyInformation((GetTopoInfoNMsg) msg);
			break;
		case TOPOLOGY_INFORMATION:
			processTopologyInformation((TopoInfoNMsg) msg);
			break;
		case ALLOCATE_PARTITION:
			processAllocatePartition((AllocPartNMsg) msg);
			break;
		case ALLOCATE_PARTITION_ERROR:
			processAllocatePartitionError((AllocPartErrorNMsg) msg);
			break;
		case PARTITION_ALLOCATED:
			processPartitionAllocated((PartAllocedNMsg) msg);
			break;
		case GET_STATUS:
			processGetStatus((GetStatusNMsg) msg);
			break;
		case GET_STATUS_ERROR:
			processGetStatusError((GetStatusErrorNMsg) msg);
			break;
		case STATUS:
			processStatus((StatusNMsg) msg);
			break;
		case DEALLOCATE_PARTITION:
			processDeallocatePartition((DeallocPartNMsg) msg);
			break;
		case DEALLOCATE_PARTITION_ERROR:
			processDeallocatePartitionError((DeallocPartErrorNMsg) msg);
			break;
		case PARTITION_DEALLOCATED:
			processPartitionDeallocated((PartDeallocedNMsg) msg);
			break;
		case TERMINATE_NODE:
			processTerminateNode((TermNodeNMsg) msg);
			break;
		case NODE_FAULT:
			processNodeFault((NodeFaultNMsg) msg);
			break;
		case TOPOLOGY_FAULT:
			processTopologyFault((TopoFaultNMsg)msg);
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
			if(msg.getGroupOp()!=null) {
				/*
				 * hook up our comms for sending replies.
				 */
				msg.getGroupOp().setComms(node.getComms());
			}
			switch(msg.getType()) {
			case ACCEPTING_JOIN:
				processAcceptingJoin((AcceptingJoinNMsg) msg);
				break;
			case JOIN_COMPLETE:
				processJoinComplete((JoinCompleteNMsg) msg);
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
			case TERMINATE_TOPOLOGY:
			case TERMINATE_TOPOLOGY_ERROR:
			case TOPOLOGY_HALTED:
			case TOPOLOGY_INFORMATION:
			case TOPOLOGY_READY:
			case TOPOLOGY_REMOVED:
			case TOPOLOGY_RESUMED:
			case TOPOLOGY_STARTED:
			case TOPOLOGY_TERMINATED:
			case ALLOCATE_PARTITION:
			case ALLOCATE_PARTITION_ERROR:
			case PARTITION_ALLOCATED:
			case GET_STATUS:
			case GET_STATUS_ERROR:
			case STATUS:
			case DEALLOCATE_PARTITION:
			case DEALLOCATE_PARTITION_ERROR:
			case PARTITION_DEALLOCATED:
			case TERMINATE_NODE:
			case NODE_FAULT:
			case TOPOLOGY_FAULT:
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
