package dragon.network;


import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.LocalCluster;
import dragon.network.Node.NodeState;
import dragon.network.messages.node.AcceptingJoinMessage;
import dragon.network.messages.node.ContextUpdateMessage;
import dragon.network.messages.node.JoinCompleteMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PrepareFailedMessage;
import dragon.network.messages.node.PrepareTopologyMessage;
import dragon.network.messages.node.StartTopologyMessage;
import dragon.network.messages.node.TopologyReadyMessage;
import dragon.network.messages.service.RunFailedMessage;
import dragon.network.messages.service.TopologySubmittedMessage;

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
		log.debug("next pointer = ["+this.nextNode+"]");
		context.put(nextNode);
		pendingJoinRequests = new HashSet<NodeMessage>();
		log.debug("starting node processor");
		start();
	}
	
	@Override
	public void run() {
		while(!shouldTerminate) {
			NodeMessage message = node.getComms().receiveNodeMessage();
			log.debug("received ["+message.getType().name()+"] from ["+message.getSender());
			switch(message.getType()) {
			case JOIN_REQUEST:
				if(node.getNodeState()!=NodeState.OPERATIONAL) {
					log.debug("placing joing request from ["+message.getSender()+"] on pending list");
					pendingJoinRequests.add(message);
				} else {
					node.setNodeState(NodeState.ACCEPTING_JOIN);
					context.put(message.getSender());
					node.getComms().sendNodeMessage(message.getSender(),new AcceptingJoinMessage(nextNode,context));
					nextNode=message.getSender();
					log.debug("next pointer = ["+nextNode+"]");
				}
				break;
			case ACCEPTING_JOIN:
				if(node.getNodeState()!=NodeState.JOIN_REQUESTED) {
					log.error("unexpected message: "+NodeMessage.NodeMessageType.ACCEPTING_JOIN.name());
				} else {
					AcceptingJoinMessage aj = (AcceptingJoinMessage) message;
					nextNode=aj.nextNode;
					log.debug("next pointer = ["+nextNode+"]");
					node.getComms().sendNodeMessage(message.getSender(), new JoinCompleteMessage());
					context.putAll(aj.context);
					for(NodeDescriptor descriptor : context.values()) {
						if(!descriptor.equals(node.getComms().getMyNodeDescriptor())) {
							node.getComms().sendNodeMessage(descriptor, new ContextUpdateMessage(context));
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
						node.getComms().sendNodeMessage(message.getSender(), new ContextUpdateMessage(context));
						hit=true;
						break;
					}
				}
				if(!hit) context.putAll(cu.context);
				break;
			case PREPARE_TOPOLOGY:
				PrepareTopologyMessage pt = (PrepareTopologyMessage) message;
				if(!node.storeJarFile(pt.topologyName,pt.jarFile)) {
					node.getComms().sendNodeMessage(pt.getSender(), new PrepareFailedMessage(pt.topologyName,"could not store the topology jar"));
					continue;
				}
				if(!node.loadJarFile(pt.topologyName)) {
					node.getComms().sendNodeMessage(pt.getSender(), new PrepareFailedMessage(pt.topologyName,"could not load the topology jar"));
					continue;
				}
				LocalCluster cluster=new LocalCluster(node);
				cluster.submitTopology(pt.topologyName, pt.conf, pt.topology, false);
				node.getRouter().submitTopology(pt.topologyName,pt.topology);
				node.getLocalClusters().put(pt.topologyName, cluster);
				node.getComms().sendNodeMessage(pt.getSender(), new TopologyReadyMessage(pt.topologyName));
				break;
			case TOPOLOGY_READY:
				TopologyReadyMessage tr = (TopologyReadyMessage) message;
				if(node.checkStartupTopology(tr.getSender(),tr.topologyId)) {
					node.getComms().sendServiceMessage(new TopologySubmittedMessage(tr.topologyId));
				}
				break;
			case START_TOPOLOGY:
				StartTopologyMessage st = (StartTopologyMessage) message;
				node.startTopology(st.topologyId);
				break;
			case PREPARE_FAILED:
				PrepareFailedMessage pf = (PrepareFailedMessage) message;
				node.removeStartupTopology(pf.topologyId);
				node.getComms().sendServiceMessage(new RunFailedMessage(pf.topologyId,pf.error));
			}
		}
	}
	
	private void processPendingJoins() {
		if(pendingJoinRequests.size()>0) {
			NodeMessage m = (NodeMessage) pendingJoinRequests.toArray()[0];
			node.setNodeState(NodeState.ACCEPTING_JOIN);
			context.put(m.getSender());
			node.getComms().sendNodeMessage(m.getSender(),new AcceptingJoinMessage(nextNode,context));
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
