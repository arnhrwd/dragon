package dragon.network;


import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.DragonRequiresClonableException;
import dragon.network.Node.NodeState;
import dragon.network.comms.DragonCommsException;
import dragon.network.messages.node.AcceptingJoinMessage;
import dragon.network.messages.node.ContextUpdateMessage;
import dragon.network.messages.node.HaltTopologyMessage;
import dragon.network.messages.node.JarReadyMessage;
import dragon.network.messages.node.JoinCompleteMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PrepareJarErrorMessage;
import dragon.network.messages.node.PrepareJarMessage;
import dragon.network.messages.node.PrepareTopologyMessage;
import dragon.network.messages.node.StartTopologyMessage;
import dragon.network.messages.node.StopTopologyErrorMessage;
import dragon.network.messages.node.StopTopologyMessage;
import dragon.network.messages.node.TopologyReadyMessage;
import dragon.network.messages.node.TopologyStartedMessage;
import dragon.network.messages.node.TopologyStoppedMessage;

public class NodeProcessor extends Thread {
	private static Log log = LogFactory.getLog(NodeProcessor.class);
	private boolean shouldTerminate=false;
	private Node node;
	private HashSet<NodeMessage> pendingJoinRequests;
	private NodeDescriptor nextNode=null;
	private NodeContext context;
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
			case JOIN_REQUEST:{
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
				break;
			}
			case ACCEPTING_JOIN:{
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
				break;
			}
			case JOIN_COMPLETE:{
				if(node.getNodeState()!=NodeState.ACCEPTING_JOIN) {
					log.error("unexpected message: "+NodeMessage.NodeMessageType.JOIN_COMPLETE.name());
				} else {
					processPendingJoins();
				}
				break;
			}
			case CONTEXT_UPDATE:{
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
				break;
			}
			case PREPARE_JAR:{
				PrepareJarMessage pjf = (PrepareJarMessage) message;
				if(!node.storeJarFile(pjf.topologyName,pjf.topologyJar)) {
					pjf.getGroupOperation().sendError(node.getComms(),"could not store the topology jar");
					continue;
				} else if(!node.loadJarFile(pjf.topologyName)) {
					pjf.getGroupOperation().sendError(node.getComms(), "could not load the topology jar");
					continue;
				}
				pjf.getGroupOperation().sendSuccess(node.getComms());
				break;
			}
			case PREPARE_JAR_ERROR:{
				PrepareJarErrorMessage pjem = (PrepareJarErrorMessage) message;
				node.getGroupOperation(pjem.getGroupOperation()
						.getId()).receiveError(node.getComms(), pjem.getSender(), pjem.error);
				break;
			}
			case JAR_READY:{
				JarReadyMessage jrm = (JarReadyMessage) message;
				node.getGroupOperation(jrm.getGroupOperation()
						.getId()).receiveSuccess(node.getComms(), jrm.getSender());
				node.removeGroupOperation(jrm.getGroupOperation().getId());
				break;
			}
			case PREPARE_TOPOLOGY:{
				PrepareTopologyMessage pt = (PrepareTopologyMessage) message;
				try {
					node.prepareTopology(pt.topologyName, pt.conf, pt.topology, false);
					pt.getGroupOperation().sendSuccess(node.getComms());
				} catch (DragonRequiresClonableException e) {
					pt.getGroupOperation().sendError(node.getComms(),e.getMessage());
				}
				break;
			}
			case TOPOLOGY_READY:{
				TopologyReadyMessage tr = (TopologyReadyMessage) message;
				node.getGroupOperation(tr.getGroupOperation().getId()).receiveSuccess(node.getComms(), tr.getSender());
				node.removeGroupOperation(tr.getGroupOperation().getId());
				break;
			}
			case START_TOPOLOGY:{
				StartTopologyMessage st = (StartTopologyMessage) message;
				node.startTopology(st.topologyId);
				st.getGroupOperation().sendSuccess(node.getComms());
				break;
			}
			case TOPOLOGY_STARTED:{
				TopologyStartedMessage tsm = (TopologyStartedMessage) message;
				node.getGroupOperation(tsm.getGroupOperation().getId()).receiveSuccess(node.getComms(), tsm.getSender());
				node.removeGroupOperation(tsm.getGroupOperation().getId());
				break;
			}
			case STOP_TOPOLOGY:{
				StopTopologyMessage stm = (StopTopologyMessage) message;
				if(!node.getLocalClusters().containsKey(stm.topologyId)){
					stm.getGroupOperation().sendError(node.getComms(),
							"topology does not exist");
				} else {
					node.stopTopology(stm.topologyId,stm.getGroupOperation());
				}
				break;
			}
			case TOPOLOGY_STOPPED:{
				TopologyStoppedMessage tsm = (TopologyStoppedMessage) message;
				node.getGroupOperation(tsm
						.getGroupOperation()
						.getId())
						.receiveSuccess(node.getComms(),tsm.getSender());
				node.removeGroupOperation(tsm.getGroupOperation().getId());
				break;
			}
			case STOP_TOPOLOGY_ERROR:{
				StopTopologyErrorMessage stem = (StopTopologyErrorMessage) message;
				node.getGroupOperation(stem
						.getGroupOperation()
						.getId())
						.receiveError(node.getComms(),stem.getSender(),stem.error);
				break;
			}
			case HALT_TOPOLOGY:{
				HaltTopologyMessage htm = (HaltTopologyMessage) message;
				node.haltTopology(htm.topologyId);
				break;
			}
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
