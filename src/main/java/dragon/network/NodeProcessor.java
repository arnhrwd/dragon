package dragon.network;


import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.DragonRequiresClonableException;
import dragon.network.Node.NodeState;
import dragon.network.comms.DragonCommsException;
import dragon.network.messages.node.AcceptingJoinNMsg;
import dragon.network.messages.node.ContextUpdateNMsg;
import dragon.network.messages.node.GetTopoInfoNMsg;
import dragon.network.messages.node.HaltTopoErrorNMsg;
import dragon.network.messages.node.HaltTopoNMsg;
import dragon.network.messages.node.JarReadyNMsg;
import dragon.network.messages.node.JoinCompleteNMsg;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PrepareJarErrorNMsg;
import dragon.network.messages.node.PrepareJarNMsg;
import dragon.network.messages.node.PrepareTopoNMsg;
import dragon.network.messages.node.ResumeTopoErrorNMsg;
import dragon.network.messages.node.ResumeTopoNMsg;
import dragon.network.messages.node.TopoRemovedNMsg;
import dragon.network.messages.node.StartTopoNMsg;
import dragon.network.messages.node.StopTopoErrorNMsg;
import dragon.network.messages.node.StopTopoNMsg;
import dragon.network.messages.node.RemoveTopoErrorNMsg;
import dragon.network.messages.node.RemoveTopoNMsg;
import dragon.network.messages.node.TopoInfoNMsg;
import dragon.network.messages.node.TopoReadyNMsg;
import dragon.network.messages.node.TopoResumedNMsg;
import dragon.network.messages.node.TopoStartedNMsg;
import dragon.network.messages.node.TopoStoppedNMsg;
import dragon.network.messages.node.TopoHaltedNMsg;
import dragon.network.operations.ListToposGroupOp;

/**
 * Process node message, which are messages that can only originate from
 * other nodes (daemons). Reads nodes messages from the Comms layer, one
 * at a time.
 * @author aaron
 *
 */
public class NodeProcessor extends Thread {
	private final static Log log = LogFactory.getLog(NodeProcessor.class);
	private boolean shouldTerminate=false;
	private final Node node;
	private final HashSet<NodeMessage> pendingJoinRequests;
	private NodeDescriptor nextNode=null;
	private final NodeContext context;
	public NodeProcessor(Node node) {
		this.node=node;
		context=new NodeContext();
		nextNode=node.getComms().getMyNodeDescriptor();
		log.info("next pointer = ["+this.nextNode+"]");
		context.put(nextNode);
		pendingJoinRequests = new HashSet<NodeMessage>();
		log.info("starting node processor");
		start();
	}
	
	private void processJoinRequest(NodeMessage message) {
		if(node.getNodeState()!=NodeState.OPERATIONAL) {
			log.debug("placing join request from ["+message.getSender()+"] on pending list");
			pendingJoinRequests.add(message);
		} else {
			node.setNodeState(NodeState.ACCEPTING_JOIN);
			context.put(message.getSender());
			try {
				node.getComms().sendNodeMessage(message.getSender(),new AcceptingJoinNMsg(nextNode,context));
				nextNode=message.getSender();
				log.debug("next pointer = ["+nextNode+"]");
			} catch (DragonCommsException e) {
				log.error("a join request could not be completed to ["+message.getSender()+"]");
				context.remove(message.getSender());
				node.setNodeState(NodeState.OPERATIONAL);
			}
		}
	}
	
	private void processAcceptingJoin(NodeMessage message) {
		if(node.getNodeState()!=NodeState.JOIN_REQUESTED) {
			log.error("unexpected message: "+NodeMessage.NodeMessageType.ACCEPTING_JOIN.name());
		} else {
			AcceptingJoinNMsg aj = (AcceptingJoinNMsg) message;
			nextNode=aj.nextNode;
			log.debug("next pointer = ["+nextNode+"]");
			try {
				node.getComms().sendNodeMessage(message.getSender(), new JoinCompleteNMsg());
			} catch (DragonCommsException e) {
				log.error("could not complete join with ["+message.getSender());
				// TODO: possibly signal that the node has failed
			}
			context.putAll(aj.context);
			for(NodeDescriptor descriptor : context.values()) {
				if(!descriptor.equals(node.getComms().getMyNodeDescriptor())) {
					try {
						node.getComms().sendNodeMessage(descriptor, new ContextUpdateNMsg(context));
					} catch (DragonCommsException e) {
						log.error("could not send context update to ["+descriptor+"]");
						// TODO: possibly signal that the node has failed
					}
				}
			}
			processPendingJoins();
		}
	}
	
	private void processJoinComplete(NodeMessage message) {
		if(node.getNodeState()!=NodeState.ACCEPTING_JOIN) {
			log.error("unexpected message: "+NodeMessage.NodeMessageType.JOIN_COMPLETE.name());
		} else {
			processPendingJoins();
		}
	}
	
	private void processContextUpdate(NodeMessage message) {
		ContextUpdateNMsg cu = (ContextUpdateNMsg) message;
		boolean hit=false;
		for(String key : context.keySet()) {
			if(!cu.context.containsKey(key)) {
				context.putAll(cu.context);
				try {
					node.getComms().sendNodeMessage(message.getSender(), new ContextUpdateNMsg(context));
				} catch (DragonCommsException e) {
					log.error("could not send context update to ["+message.getSender()+"]");
					// TODO: possibly signal that the node has failed
				}
				hit=true;
				break;
			}
		}
		if(!hit) context.putAll(cu.context);
	}
	
	private void processPrepareJar(NodeMessage message) {
		PrepareJarNMsg pjf = (PrepareJarNMsg) message;
		if(!node.storeJarFile(pjf.topologyName,pjf.topologyJar)) {
			pjf.getGroupOperation().sendError(node.getComms(),"could not store the topology jar");
			return;
		} else if(!node.loadJarFile(pjf.topologyName)) {
			pjf.getGroupOperation().sendError(node.getComms(), "could not load the topology jar");
			return;
		}
		pjf.getGroupOperation().sendSuccess(node.getComms());
	}
	
	private void processPrepareJarError(NodeMessage message) {
		PrepareJarErrorNMsg pjem = (PrepareJarErrorNMsg) message;
		node.getOperationsProcessor().getGroupOperation(pjem.getGroupOperation()
				.getId()).receiveError(node.getComms(), pjem.getSender(), pjem.error);
	}
	
	private void processJarReady(NodeMessage message) {
		JarReadyNMsg jrm = (JarReadyNMsg) message;
		node.getOperationsProcessor().getGroupOperation(jrm.getGroupOperation()
				.getId()).receiveSuccess(node.getComms(), jrm.getSender());
		//node.removeGroupOperation(jrm.getGroupOperation().getId());
	}
	
	private void processPrepareTopology(NodeMessage message) {
		PrepareTopoNMsg pt = (PrepareTopoNMsg) message;
		try {
			node.prepareTopology(pt.topologyName, pt.conf, pt.topology, false);
			pt.getGroupOperation().sendSuccess(node.getComms());
		} catch (DragonRequiresClonableException e) {
			pt.getGroupOperation().sendError(node.getComms(),e.getMessage());
		}
	}
	
	private void processTopologyReady(NodeMessage message) {
		TopoReadyNMsg tr = (TopoReadyNMsg) message;
		node.getOperationsProcessor().getGroupOperation(tr.getGroupOperation().getId()).receiveSuccess(node.getComms(), tr.getSender());
		//node.removeGroupOperation(tr.getGroupOperation().getId());
	}
	
	private void processStartTopology(NodeMessage message) {
		StartTopoNMsg st = (StartTopoNMsg) message;
		node.startTopology(st.topologyId);
		st.getGroupOperation().sendSuccess(node.getComms());
	}
	
	private void processTopologyStarted(NodeMessage message) {
		TopoStartedNMsg tsm = (TopoStartedNMsg) message;
		node.getOperationsProcessor().getGroupOperation(tsm.getGroupOperation().getId()).receiveSuccess(node.getComms(), tsm.getSender());
		//node.removeGroupOperation(tsm.getGroupOperation().getId());
	}
	
	private void processStopTopology(NodeMessage message) {
		StopTopoNMsg stm = (StopTopoNMsg) message;
		if(!node.getLocalClusters().containsKey(stm.topologyId)){
			stm.getGroupOperation().sendError(node.getComms(),
					"topology does not exist");
		} else {
			node.stopTopology(stm.topologyId,stm.getGroupOperation());
		}
	}
	

	
	private void processTopologyStopped(NodeMessage message) {
		TopoStoppedNMsg tsm = (TopoStoppedNMsg) message;
		node.getOperationsProcessor().getGroupOperation(tsm
				.getGroupOperation()
				.getId())
				.receiveSuccess(node.getComms(),tsm.getSender());
		//node.removeGroupOperation(tsm.getGroupOperation().getId());
	}
	
	private void processStopTopologyError(NodeMessage message) {
		StopTopoErrorNMsg stem = (StopTopoErrorNMsg) message;
		node.getOperationsProcessor().getGroupOperation(stem
				.getGroupOperation()
				.getId())
				.receiveError(node.getComms(),stem.getSender(),stem.error);
	}
	
	private void processTerminateRouter(NodeMessage message) {
		RemoveTopoNMsg trm = (RemoveTopoNMsg) message;
		node.removeTopo(trm.topologyId);
		if(trm.getGroupOperation()!=null) {
			trm.getGroupOperation().sendSuccess(node.getComms());
		}
	}
	
	private void processRouterTerminated(NodeMessage message) {
		TopoRemovedNMsg rtm = (TopoRemovedNMsg) message;
		node.getOperationsProcessor().getGroupOperation(rtm
				.getGroupOperation()
				.getId())
				.receiveSuccess(node.getComms(),rtm.getSender());
	}
	
	private void processTerminateRouterError(NodeMessage message) {
		RemoveTopoErrorNMsg trm = (RemoveTopoErrorNMsg) message;
		node.getOperationsProcessor().getGroupOperation(trm.getGroupOperation().getId()).receiveError(node.getComms(), trm.getSender(),trm.error);
	}
	
	private void processHaltTopology(NodeMessage message) {
		HaltTopoNMsg htm = (HaltTopoNMsg) message;
		node.haltTopology(htm.topologyId);
		if(htm.getGroupOperation()!=null) {
			htm.getGroupOperation().sendSuccess(node.getComms());
		}
	}
	
	private void processTopologyHalted(NodeMessage message) {
		TopoHaltedNMsg thm = (TopoHaltedNMsg) message;
		node.getOperationsProcessor().getGroupOperation(thm.getGroupOperation().getId()).receiveSuccess(node.getComms(), thm.getSender());
	}
	
	private void processHaltTopologyError(NodeMessage message) {
		HaltTopoErrorNMsg htem = (HaltTopoErrorNMsg) message;
		node.getOperationsProcessor().getGroupOperation(htem
				.getGroupOperation()
				.getId())
				.receiveError(node.getComms(), htem.getSender(), htem.error);
	}
	
	private void processResumeTopology(NodeMessage message) {
		ResumeTopoNMsg htm = (ResumeTopoNMsg) message;
		node.resumeTopology(htm.topologyId);
		if(htm.getGroupOperation()!=null) {
			htm.getGroupOperation().sendSuccess(node.getComms());
		}
	}
	
	private void processTopologyResumed(NodeMessage message) {
		TopoResumedNMsg thm = (TopoResumedNMsg) message;
		node.getOperationsProcessor().getGroupOperation(thm.getGroupOperation().getId()).receiveSuccess(node.getComms(), thm.getSender());
	}
	
	private void processResumeTopologyError(NodeMessage message) {
		ResumeTopoErrorNMsg htem = (ResumeTopoErrorNMsg) message;
		node.getOperationsProcessor().getGroupOperation(htem
				.getGroupOperation()
				.getId())
				.receiveError(node.getComms(), htem.getSender(), htem.error);
	}
	
	private void processGetTopologyInformation(NodeMessage message) {
		GetTopoInfoNMsg gtim = (GetTopoInfoNMsg) message;
		node.listTopologies((ListToposGroupOp)gtim.getGroupOperation());
	}
	
	private void processTopologyInformation(NodeMessage message) {
		TopoInfoNMsg tim = (TopoInfoNMsg) message;
		((ListToposGroupOp)(node.getOperationsProcessor().getGroupOperation(tim.getGroupOperation().getId()))).aggregate(tim.getSender(),
				tim.state,tim.errors);
		node.getOperationsProcessor().getGroupOperation(tim.getGroupOperation().getId()).receiveSuccess(node.getComms(),
				tim.getSender());
	}
	
	@Override
	public void run() {
		while(!shouldTerminate) {
			NodeMessage message;
			try {
				message = node.getComms().receiveNodeMessage();
			} catch (InterruptedException e) {
				log.info("interrupted");
				break;
			}
			log.debug("received ["+message.getType().name()+"] from ["+message.getSender());
			switch(message.getType()) {
			case JOIN_REQUEST:
				processJoinRequest(message);
				break;
			case ACCEPTING_JOIN:
				processAcceptingJoin(message);
				break;
			case JOIN_COMPLETE:
				processJoinComplete(message);
				break;
			case CONTEXT_UPDATE:
				processContextUpdate(message);
				break;
			case PREPARE_JAR:
				processPrepareJar(message);
				break;
			case PREPARE_JAR_ERROR:
				processPrepareJarError(message);
				break;
			case JAR_READY:
				processJarReady(message);
				break;
			case PREPARE_TOPOLOGY:
				processPrepareTopology(message);
				break;
			case TOPOLOGY_READY:
				processTopologyReady(message);
				break;
			case START_TOPOLOGY:
				processStartTopology(message);
				break;
			case TOPOLOGY_STARTED:
				processTopologyStarted(message);
				break;
			case STOP_TOPOLOGY:
				processStopTopology(message);
				break;
			case TOPOLOGY_STOPPED:
				processTopologyStopped(message);
				break;
			case STOP_TOPOLOGY_ERROR:
				processStopTopologyError(message);
				break;
			case REMOVE_TOPOLOGY:
				processTerminateRouter(message);
				break;
			case TOPOLOGY_REMOVED:
				processRouterTerminated(message);
				break;
			case REMOVE_TOPOLOGY_ERROR:
				processTerminateRouterError(message);
				break;
			case HALT_TOPOLOGY:
				processHaltTopology(message);
				break;
			case TOPOLOGY_HALTED:
				processTopologyHalted(message);
				break;
			case HALT_TOPOLOGY_ERROR:
				processHaltTopologyError(message);
				break;
			case RESUME_TOPOLOGY:
				processResumeTopology(message);
				break;
			case TOPOLOGY_RESUMED:
				processTopologyResumed(message);
				break;
			case RESUME_TOPOLOGY_ERROR:
				processResumeTopologyError(message);
				break;
			case GET_TOPOLOGY_INFORMATION:
				processGetTopologyInformation(message);
				break;
			case TOPOLOGY_INFORMATION:
				processTopologyInformation(message);
				break;
			default:
				break;
			}
		}
	}
	
	private void processPendingJoins() {
		if(pendingJoinRequests.size()>0) {
			NodeMessage m = (NodeMessage) pendingJoinRequests.toArray()[0];
			node.setNodeState(NodeState.ACCEPTING_JOIN);
			context.put(m.getSender());
			try {
				node.getComms().sendNodeMessage(m.getSender(),new AcceptingJoinNMsg(nextNode,context));
			} catch (DragonCommsException e) {
				log.error("could not send accepting join to ["+m.getSender()+"]");
			}
			nextNode=m.getSender();
			pendingJoinRequests.remove(m);
		} else {
			node.setNodeState(NodeState.OPERATIONAL);
		}
	}
	
	public NodeContext getContext() {
		return context;
	}
}
