package dragon.network;


import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.DragonRequiresClonableException;
import dragon.network.Node.NodeState;
import dragon.network.comms.DragonCommsException;
import dragon.network.messages.node.AcceptingJoinMessage;
import dragon.network.messages.node.ContextUpdateMessage;
import dragon.network.messages.node.GetTopologyInformationMessage;
import dragon.network.messages.node.HaltTopologyErrorMessage;
import dragon.network.messages.node.HaltTopologyMessage;
import dragon.network.messages.node.JarReadyMessage;
import dragon.network.messages.node.JoinCompleteMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PrepareJarErrorMessage;
import dragon.network.messages.node.PrepareJarMessage;
import dragon.network.messages.node.PrepareTopologyMessage;
import dragon.network.messages.node.ResumeTopologyErrorMessage;
import dragon.network.messages.node.ResumeTopologyMessage;
import dragon.network.messages.node.StartTopologyMessage;
import dragon.network.messages.node.StopTopologyErrorMessage;
import dragon.network.messages.node.StopTopologyMessage;
import dragon.network.messages.node.TopologyInformationMessage;
import dragon.network.messages.node.TopologyReadyMessage;
import dragon.network.messages.node.TopologyResumedMessage;
import dragon.network.messages.node.TopologyStartedMessage;
import dragon.network.messages.node.TopologyStoppedMessage;
import dragon.network.messages.node.TopologyHaltedMessage;
import dragon.network.operations.ListTopologiesGroupOperation;

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
				node.getComms().sendNodeMessage(message.getSender(),new AcceptingJoinMessage(nextNode,context));
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
			AcceptingJoinMessage aj = (AcceptingJoinMessage) message;
			nextNode=aj.nextNode;
			log.debug("next pointer = ["+nextNode+"]");
			try {
				node.getComms().sendNodeMessage(message.getSender(), new JoinCompleteMessage());
			} catch (DragonCommsException e) {
				log.error("could not complete join with ["+message.getSender());
				// TODO: possibly signal that the node has failed
			}
			context.putAll(aj.context);
			for(NodeDescriptor descriptor : context.values()) {
				if(!descriptor.equals(node.getComms().getMyNodeDescriptor())) {
					try {
						node.getComms().sendNodeMessage(descriptor, new ContextUpdateMessage(context));
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
		ContextUpdateMessage cu = (ContextUpdateMessage) message;
		boolean hit=false;
		for(String key : context.keySet()) {
			if(!cu.context.containsKey(key)) {
				context.putAll(cu.context);
				try {
					node.getComms().sendNodeMessage(message.getSender(), new ContextUpdateMessage(context));
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
		PrepareJarMessage pjf = (PrepareJarMessage) message;
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
		PrepareJarErrorMessage pjem = (PrepareJarErrorMessage) message;
		node.getGroupOperation(pjem.getGroupOperation()
				.getId()).receiveError(node.getComms(), pjem.getSender(), pjem.error);
	}
	
	private void processJarReady(NodeMessage message) {
		JarReadyMessage jrm = (JarReadyMessage) message;
		node.getGroupOperation(jrm.getGroupOperation()
				.getId()).receiveSuccess(node.getComms(), jrm.getSender());
		//node.removeGroupOperation(jrm.getGroupOperation().getId());
	}
	
	private void processPrepareTopology(NodeMessage message) {
		PrepareTopologyMessage pt = (PrepareTopologyMessage) message;
		try {
			node.prepareTopology(pt.topologyName, pt.conf, pt.topology, false);
			pt.getGroupOperation().sendSuccess(node.getComms());
		} catch (DragonRequiresClonableException e) {
			pt.getGroupOperation().sendError(node.getComms(),e.getMessage());
		}
	}
	
	private void processTopologyReady(NodeMessage message) {
		TopologyReadyMessage tr = (TopologyReadyMessage) message;
		node.getGroupOperation(tr.getGroupOperation().getId()).receiveSuccess(node.getComms(), tr.getSender());
		//node.removeGroupOperation(tr.getGroupOperation().getId());
	}
	
	private void processStartTopology(NodeMessage message) {
		StartTopologyMessage st = (StartTopologyMessage) message;
		node.startTopology(st.topologyId);
		st.getGroupOperation().sendSuccess(node.getComms());
	}
	
	private void processTopologyStarted(NodeMessage message) {
		TopologyStartedMessage tsm = (TopologyStartedMessage) message;
		node.getGroupOperation(tsm.getGroupOperation().getId()).receiveSuccess(node.getComms(), tsm.getSender());
		//node.removeGroupOperation(tsm.getGroupOperation().getId());
	}
	
	private void processStopTopology(NodeMessage message) {
		StopTopologyMessage stm = (StopTopologyMessage) message;
		if(!node.getLocalClusters().containsKey(stm.topologyId)){
			stm.getGroupOperation().sendError(node.getComms(),
					"topology does not exist");
		} else {
			node.stopTopology(stm.topologyId,stm.getGroupOperation());
		}
	}
	
	private void processTopologyStopped(NodeMessage message) {
		TopologyStoppedMessage tsm = (TopologyStoppedMessage) message;
		node.getGroupOperation(tsm
				.getGroupOperation()
				.getId())
				.receiveSuccess(node.getComms(),tsm.getSender());
		//node.removeGroupOperation(tsm.getGroupOperation().getId());
	}
	
	private void processStopTopologyError(NodeMessage message) {
		StopTopologyErrorMessage stem = (StopTopologyErrorMessage) message;
		node.getGroupOperation(stem
				.getGroupOperation()
				.getId())
				.receiveError(node.getComms(),stem.getSender(),stem.error);
	}
	
	private void processHaltTopology(NodeMessage message) {
		HaltTopologyMessage htm = (HaltTopologyMessage) message;
		node.haltTopology(htm.topologyId);
		if(htm.getGroupOperation()!=null) {
			htm.getGroupOperation().sendSuccess(node.getComms());
		}
	}
	
	private void processTopologyHalted(NodeMessage message) {
		TopologyHaltedMessage thm = (TopologyHaltedMessage) message;
		node.getGroupOperation(thm.getGroupOperation().getId()).receiveSuccess(node.getComms(), thm.getSender());
	}
	
	private void processHaltTopologyError(NodeMessage message) {
		HaltTopologyErrorMessage htem = (HaltTopologyErrorMessage) message;
		node.getGroupOperation(htem
				.getGroupOperation()
				.getId())
				.receiveError(node.getComms(), htem.getSender(), htem.error);
	}
	
	private void processResumeTopology(NodeMessage message) {
		ResumeTopologyMessage htm = (ResumeTopologyMessage) message;
		node.resumeTopology(htm.topologyId);
		if(htm.getGroupOperation()!=null) {
			htm.getGroupOperation().sendSuccess(node.getComms());
		}
	}
	
	private void processTopologyResumed(NodeMessage message) {
		TopologyResumedMessage thm = (TopologyResumedMessage) message;
		node.getGroupOperation(thm.getGroupOperation().getId()).receiveSuccess(node.getComms(), thm.getSender());
	}
	
	private void processResumeTopologyError(NodeMessage message) {
		ResumeTopologyErrorMessage htem = (ResumeTopologyErrorMessage) message;
		node.getGroupOperation(htem
				.getGroupOperation()
				.getId())
				.receiveError(node.getComms(), htem.getSender(), htem.error);
	}
	
	private void processGetTopologyInformation(NodeMessage message) {
		GetTopologyInformationMessage gtim = (GetTopologyInformationMessage) message;
		node.listTopologies((ListTopologiesGroupOperation)gtim.getGroupOperation());
	}
	
	private void processTopologyInformation(NodeMessage message) {
		TopologyInformationMessage tim = (TopologyInformationMessage) message;
		((ListTopologiesGroupOperation)(node.getGroupOperation(tim.getGroupOperation().getId()))).aggregate(tim.getSender(),
				tim.state,tim.errors);
		node.getGroupOperation(tim.getGroupOperation().getId()).receiveSuccess(node.getComms(),
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
				node.getComms().sendNodeMessage(m.getSender(),new AcceptingJoinMessage(nextNode,context));
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
