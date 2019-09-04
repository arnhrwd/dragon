package dragon.network;

import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.Node.NodeState;
import dragon.network.messages.node.AcceptingJoin;
import dragon.network.messages.node.JoinComplete;
import dragon.network.messages.node.NodeMessage;

public class NodeProcessor extends Thread {
	private static Log log = LogFactory.getLog(NodeProcessor.class);
	private boolean shouldTerminate=false;
	private Node node;
	private HashSet<NodeMessage> pendingJoinRequests;
	private NodeDescriptor nextNode=null;
	public NodeProcessor(Node node) {
		this.node=node;
		this.nextNode=node.getComms().getMyNodeDescriptor();
		pendingJoinRequests = new HashSet<NodeMessage>();
		log.debug("starting node processor");
		start();
	}
	public void run() {
		while(!shouldTerminate) {
			NodeMessage message = node.getComms().receiveNodeMessage();
			switch(message.getType()) {
			case JOIN_REQUEST:
				if(node.getNodeState()!=NodeState.OPERATIONAL) {
					pendingJoinRequests.add(message);
				} else {
					node.setNodeState(NodeState.ACCEPTING_JOIN);
					node.getComms().sendNodeMessage(message.getSender(),
							new AcceptingJoin(nextNode));
					nextNode=message.getSender();
				}
				break;
			case ACCEPTING_JOIN:
				if(node.getNodeState()!=NodeState.JOIN_REQUESTED) {
					log.error("unexpected message: "+NodeMessage.NodeMessageType.ACCEPTING_JOIN.name());
				} else {
					AcceptingJoin aj = (AcceptingJoin) message;
					nextNode=aj.nextNode;
					node.getComms().sendNodeMessage(message.getSender(), new JoinComplete());
					node.setNodeState(NodeState.OPERATIONAL);
				}
			case JOIN_COMPLETE:
				if(node.getNodeState()!=NodeState.ACCEPTING_JOIN) {
					log.error("unexpected message: "+NodeMessage.NodeMessageType.JOIN_COMPLETE.name());
				} else {
					node.setNodeState(NodeState.OPERATIONAL);
				}
			}
		}
	}
}
