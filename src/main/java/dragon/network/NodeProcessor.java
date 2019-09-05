package dragon.network;


import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.Node.NodeState;
import dragon.network.messages.node.AcceptingJoinMessage;
import dragon.network.messages.node.ContextUpdateMessage;
import dragon.network.messages.node.JoinCompleteMessage;
import dragon.network.messages.node.NodeMessage;

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
		this.nextNode=node.getComms().getMyNodeDescriptor();
		context.put(this.nextNode);
		pendingJoinRequests = new HashSet<NodeMessage>();
		log.debug("starting node processor");
		start();
	}
	
	@Override
	public void run() {
		while(!shouldTerminate) {
			NodeMessage message = node.getComms().receiveNodeMessage();
			switch(message.getType()) {
			case JOIN_REQUEST:
				if(node.getNodeState()!=NodeState.OPERATIONAL) {
					pendingJoinRequests.add(message);
				} else {
					node.setNodeState(NodeState.ACCEPTING_JOIN);
					context.put(message.getSender());
					node.getComms().sendNodeMessage(message.getSender(),new AcceptingJoinMessage(nextNode,context));
					nextNode=message.getSender();
				}
				break;
			case ACCEPTING_JOIN:
				if(node.getNodeState()!=NodeState.JOIN_REQUESTED) {
					log.error("unexpected message: "+NodeMessage.NodeMessageType.ACCEPTING_JOIN.name());
				} else {
					AcceptingJoinMessage aj = (AcceptingJoinMessage) message;
					nextNode=aj.nextNode;
					node.getComms().sendNodeMessage(message.getSender(), new JoinCompleteMessage());
					context.putAll(aj.context);
					for(NodeDescriptor descriptor : context.values()) {
						node.getComms().sendNodeMessage(descriptor, new ContextUpdateMessage(context));
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
