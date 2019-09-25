package dragon.network;


import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.LocalCluster;
import dragon.network.Node.NodeState;
import dragon.network.comms.DragonCommsException;
import dragon.network.messages.node.AcceptingJoinMessage;
import dragon.network.messages.node.ContextUpdateMessage;
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
import dragon.network.messages.node.TopologyStoppedMessage;
import dragon.network.messages.service.RunTopologyErrorMessage;
import dragon.network.messages.service.TerminateTopologyErrorMessage;
import dragon.network.messages.service.TopologyRunningMessage;
import dragon.topology.DragonTopology;

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
			case JOIN_REQUEST:
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
			case ACCEPTING_JOIN:
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
			case JOIN_COMPLETE:
				if(node.getNodeState()!=NodeState.ACCEPTING_JOIN) {
					log.error("unexpected message: "+NodeMessage.NodeMessageType.JOIN_COMPLETE.name());
				} else {
					processPendingJoins();
				}
				break;
			case CONTEXT_UPDATE:
				ContextUpdateMessage cu = (ContextUpdateMessage) message;
				boolean hit=false;
				for(String key : context.keySet()) {
					if(!cu.context.containsKey(key)) {
						context.putAll(cu.context);
						try {
							node.getComms().sendNodeMessage(message.getSender(), new ContextUpdateMessage(context));
						} catch (DragonCommsException e) {
							log.debug("could not send context update to ["+message.getSender()+"]");
							// TODO: possibly signal that the node has failed
						}
						hit=true;
						break;
					}
				}
				if(!hit) context.putAll(cu.context);
				break;
			case PREPARE_JAR:
				{
					PrepareJarMessage pjf = (PrepareJarMessage) message;
					if(!node.storeJarFile(pjf.topologyName,pjf.topologyJar)) {
						try {
							node.getComms().sendNodeMessage(pjf.getSender(), 
									new PrepareJarErrorMessage(pjf.topologyName,"could not store the topology jar"),
									message);
						} catch (DragonCommsException e) {
							log.debug("could not send prepare jare error message to ["+pjf.getSender()+"]");
							// TODO: possibly signal that the node has failed
						}
						continue;
					} else if(!node.loadJarFile(pjf.topologyName)) {
						try {
							node.getComms().sendNodeMessage(pjf.getSender(), 
									new PrepareJarErrorMessage(pjf.topologyName,"could not load the topology jar"),
									message);
						} catch (DragonCommsException e) {
							log.debug("could not send prepare jare error message to ["+pjf.getSender()+"]");
							// TODO: possibly signal that the node has failed
						}
						continue;
					}
					try {
						node.getComms().sendNodeMessage(message.getSender(), 
								new JarReadyMessage(pjf.topologyName),message);
					} catch (DragonCommsException e) {
						log.debug("could not send prepare jare ready message to ["+pjf.getSender()+"]");
						// TODO: possibly signal that the node has failed
					}
				}
				break;
			
			case JAR_READY:
				{
					JarReadyMessage jrm = (JarReadyMessage) message;
					NodeMessage response = new PrepareTopologyMessage(jrm.topologyId,
							node.getLocalClusters().get(jrm.topologyId).getConf(),
							node.getLocalClusters().get(jrm.topologyId).getTopology());
					try {
						node.getComms().sendNodeMessage(message.getSender(), response, message);
					} catch (DragonCommsException e) {
						log.error("could not send prepare topology message to ["+message.getSender()+"]");
						// TODO: possibly signal that the node has failed
					}
				}
				break;
			case PREPARE_TOPOLOGY:
				PrepareTopologyMessage pt = (PrepareTopologyMessage) message;
				{
					LocalCluster cluster=new LocalCluster(node);
					cluster.submitTopology(pt.topologyName, pt.conf, pt.topology, false);
					node.getRouter().submitTopology(pt.topologyName,pt.topology);
					node.getLocalClusters().put(pt.topologyName, cluster);
					try {
						node.getComms().sendNodeMessage(pt.getSender(), 
								new TopologyReadyMessage(pt.topologyName), message);
					} catch (DragonCommsException e) {
						log.error("could not send topology ready message to ["+pt.getSender()+"]");
						// TODO: clean up
					}
				}
				break;
			case TOPOLOGY_READY:
				TopologyReadyMessage tr = (TopologyReadyMessage) message;
				try {
					if(node.checkStartupTopology(tr.getSender(),tr.topologyId)) {
						node.getComms().sendServiceMessage(new TopologyRunningMessage(tr.topologyId),tr);
					}
				} catch (DragonCommsException e) {
					log.error("could not send appropriate messages when running the topology");
				}
				break;
			case START_TOPOLOGY:
				StartTopologyMessage st = (StartTopologyMessage) message;
				node.startTopology(st.topologyId);
				break;
			case PREPARE_JAR_ERROR:
				PrepareJarErrorMessage pf = (PrepareJarErrorMessage) message;
				node.removeStartupTopology(pf.topologyId);
				try {
					node.getComms().sendServiceMessage(new RunTopologyErrorMessage(pf.topologyId,pf.error),pf);
				} catch (DragonCommsException e) {
					// ignore
				}
				break;
			case STOP_TOPOLOGY:
			{
				StopTopologyMessage stm = (StopTopologyMessage) message;
				if(!node.getLocalClusters().containsKey(stm.topologyId)){
					stm.getGroupOperation().sendError(node.getComms(),
							"topology does not exist");
				} else {
					
					LocalCluster localCluster = node.getLocalClusters().get(stm.topologyId);
					localCluster.setGroupOperation(stm.getGroupOperation());
					localCluster.setShouldTerminate();
				}
			}
				break;
			case TOPOLOGY_STOPPED:
			{
				TopologyStoppedMessage tsm = (TopologyStoppedMessage) message;
				node.getGroupOperation(tsm
						.getGroupOperation()
						.getId())
						.receiveSuccess(node.getComms(),tsm.getSender());
			}
				break;
			case STOP_TOPOLOGY_ERROR:
			{
				StopTopologyErrorMessage stem = (StopTopologyErrorMessage) message;
				node.getGroupOperation(stem
						.getGroupOperation()
						.getId())
						.receiveError(node.getComms(),stem.getSender(),stem.error);
			}
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
