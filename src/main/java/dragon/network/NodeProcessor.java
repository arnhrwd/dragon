package dragon.network;


import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.DragonRequiresClonableException;
import dragon.network.Node.NodeState;
import dragon.network.comms.DragonCommsException;
import dragon.network.messages.IErrorMessage;
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
		nextNode=node.getComms().getMyNodeDesc();
		log.info("next pointer = ["+this.nextNode+"]");
		context.put(nextNode);
		pendingJoinRequests = new HashSet<NodeMessage>();
		log.info("starting node processor");
		start();
	}
	
	private void receiveError(NodeMessage msg) {
		node.getOperationsProcessor()
		.getGroupOp(msg.getGroupOp().getId())
		.receiveError(node.getComms(), 
				msg.getSender(),
				((IErrorMessage)msg).getError());
	}
	
	private void receiveSuccess(NodeMessage msg) {
		node.getOperationsProcessor()
		.getGroupOp(msg.getGroupOp().getId())
		.receiveSuccess(node.getComms(), 
				msg.getSender());
	}
	
	private void sendSuccess(NodeMessage msg) {
		msg.getGroupOp().sendSuccess(node.getComms());
	}
	
	private void sendError(NodeMessage msg,String error) {
		msg.getGroupOp().sendError(node.getComms(),error);
	}
	
	private void processJoinRequest(NodeMessage msg) {
		if(node.getNodeState()!=NodeState.OPERATIONAL) {
			log.debug("placing join request from ["+msg.getSender()+"] on pending list");
			pendingJoinRequests.add(msg);
		} else {
			node.setNodeState(NodeState.ACCEPTING_JOIN);
			context.put(msg.getSender());
			try {
				node.getComms().sendNodeMsg(msg.getSender(),new AcceptingJoinNMsg(nextNode,context));
				nextNode=msg.getSender();
				log.debug("next pointer = ["+nextNode+"]");
			} catch (DragonCommsException e) {
				log.error("a join request could not be completed to ["+msg.getSender()+"]");
				context.remove(msg.getSender());
				node.setNodeState(NodeState.OPERATIONAL);
			}
		}
	}
	
	private void processAcceptingJoin(NodeMessage msg) {
		if(node.getNodeState()!=NodeState.JOIN_REQUESTED) {
			log.error("unexpected message: "+NodeMessage.NodeMessageType.ACCEPTING_JOIN.name());
		} else {
			AcceptingJoinNMsg aj = (AcceptingJoinNMsg) msg;
			nextNode=aj.nextNode;
			log.debug("next pointer = ["+nextNode+"]");
			try {
				node.getComms().sendNodeMsg(msg.getSender(), new JoinCompleteNMsg());
			} catch (DragonCommsException e) {
				log.error("could not complete join with ["+msg.getSender());
				// TODO: possibly signal that the node has failed
			}
			context.putAll(aj.context);
			for(NodeDescriptor descriptor : context.values()) {
				if(!descriptor.equals(node.getComms().getMyNodeDesc())) {
					try {
						node.getComms().sendNodeMsg(descriptor, new ContextUpdateNMsg(context));
					} catch (DragonCommsException e) {
						log.error("could not send context update to ["+descriptor+"]");
						// TODO: possibly signal that the node has failed
					}
				}
			}
			processPendingJoins();
		}
	}
	
	private void processJoinComplete(NodeMessage msg) {
		if(node.getNodeState()!=NodeState.ACCEPTING_JOIN) {
			log.error("unexpected message: "+NodeMessage.NodeMessageType.JOIN_COMPLETE.name());
		} else {
			processPendingJoins();
		}
	}
	
	private void processContextUpdate(NodeMessage msg) {
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
	
	private void processPrepareJar(NodeMessage msg) {
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
	
	private void processPrepareJarError(NodeMessage msg) {
		PrepareJarErrorNMsg pjem = (PrepareJarErrorNMsg) msg;
		receiveError(pjem);
	}
	
	private void processJarReady(NodeMessage msg) {
		JarReadyNMsg jrm = (JarReadyNMsg) msg;
		receiveSuccess(jrm);
	}
	
	private void processPrepareTopology(NodeMessage msg) {
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
	
	private void processTopologyReady(NodeMessage msg) {
		TopoReadyNMsg tr = (TopoReadyNMsg) msg;
		receiveSuccess(tr);
	}
	
	private void processStartTopology(NodeMessage msg) {
		StartTopoNMsg st = (StartTopoNMsg) msg;
		try {
			node.startTopology(st.topologyId);
			sendSuccess(st);
		} catch (DragonTopologyException e) {
			sendError(st,e.getMessage());
		}
		
	}
	
	private void processTopologyStarted(NodeMessage msg) {
		TopoStartedNMsg tsm = (TopoStartedNMsg) msg;
		receiveSuccess(tsm);
	}
	
	private void processStopTopology(NodeMessage msg) {
		StopTopoNMsg stm = (StopTopoNMsg) msg;
		try {
			// starts a thread to stop the topology
			log.debug("asking node to stop the topology");
			node.terminateTopology(stm.topologyId,stm.getGroupOp());
		} catch (DragonTopologyException e) {
			sendError(stm,e.getMessage());
		} 
	}
		
	private void processTopologyStopped(NodeMessage msg) {
		TopoStoppedNMsg tsm = (TopoStoppedNMsg) msg;
		receiveSuccess(tsm);
	}
	
	private void processStopTopologyError(NodeMessage msg) {
		StopTopoErrorNMsg stem = (StopTopoErrorNMsg) msg;
		receiveError(stem);
	}
	
	private void processRemoveTopology(NodeMessage msg) {
		RemoveTopoNMsg trm = (RemoveTopoNMsg) msg;
		try {
			node.removeTopo(trm.topologyId);
			sendSuccess(trm);
		} catch (DragonTopologyException e) {
			sendError(trm,e.getMessage());
		}
	}
	
	private void processTopologyRemoved(NodeMessage msg) {
		TopoRemovedNMsg rtm = (TopoRemovedNMsg) msg;
		receiveSuccess(rtm);
	}
	
	private void processRemoveTopologyError(NodeMessage msg) {
		RemoveTopoErrorNMsg trm = (RemoveTopoErrorNMsg) msg;
		receiveError(trm);
	}
	
	private void processHaltTopology(NodeMessage msg) {
		HaltTopoNMsg htm = (HaltTopoNMsg) msg;
		try {
			node.haltTopology(htm.topologyId);
			sendSuccess(htm);
		} catch (DragonTopologyException e) {
			sendError(htm,e.getMessage());
		}
	}
	
	private void processTopologyHalted(NodeMessage msg) {
		TopoHaltedNMsg thm = (TopoHaltedNMsg) msg;
		receiveSuccess(thm);
	}
	
	private void processHaltTopologyError(NodeMessage msg) {
		HaltTopoErrorNMsg htem = (HaltTopoErrorNMsg) msg;
		receiveError(htem);
	}
	
	private void processResumeTopology(NodeMessage msg) {
		ResumeTopoNMsg htm = (ResumeTopoNMsg) msg;
		try {
			node.resumeTopology(htm.topologyId);
			sendSuccess(htm);
		} catch (DragonTopologyException e) {
			sendError(htm,e.getMessage());
		}
	}
	
	private void processTopologyResumed(NodeMessage msg) {
		TopoResumedNMsg thm = (TopoResumedNMsg) msg;
		receiveSuccess(thm);
	}
	
	private void processResumeTopologyError(NodeMessage msg) {
		ResumeTopoErrorNMsg htem = (ResumeTopoErrorNMsg) msg;
		receiveError(htem);
	}
	
	private void processGetTopologyInformation(NodeMessage msg) {
		GetTopoInfoNMsg gtim = (GetTopoInfoNMsg) msg;
		node.listTopologies((ListToposGroupOp)gtim.getGroupOp()); // sends the response for us
	}
	
	private void processTopologyInformation(NodeMessage msg) {
		TopoInfoNMsg tim = (TopoInfoNMsg) msg;
		((ListToposGroupOp)(node.getOperationsProcessor()
				.getGroupOp(tim.getGroupOp().getId())))
				.aggregate(tim.getSender(),tim.state,tim.errors);
		receiveSuccess(tim);
	}
	
	@Override
	public void run() {
		while(!shouldTerminate) {
			NodeMessage msg;
			try {
				msg = node.getComms().receiveNodeMsg();
			} catch (InterruptedException e) {
				log.info("interrupted");
				break;
			}
			log.debug("received ["+msg.getType().name()+"] from ["+msg.getSender());
			switch(msg.getType()) {
			case JOIN_REQUEST:
				processJoinRequest(msg);
				break;
			case ACCEPTING_JOIN:
				processAcceptingJoin(msg);
				break;
			case JOIN_COMPLETE:
				processJoinComplete(msg);
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
				node.getComms().sendNodeMsg(m.getSender(),new AcceptingJoinNMsg(nextNode,context));
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
