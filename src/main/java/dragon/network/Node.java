package dragon.network;

import java.io.IOException;
import java.util.HashMap;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.LocalCluster;
import dragon.network.messages.node.JoinRequestMessage;



public class Node {
	private static Log log = LogFactory.getLog(Node.class);
	private IComms comms;

	private HashMap<String,LocalCluster> localClusters;
	private ServiceProcessor serviceThread;
	private NodeProcessor nodeThread;
	private boolean shouldTerminate=false;
	private Config conf;
	
	private Router router;
	
	public enum NodeState {
		JOINING,
		JOIN_REQUESTED,
		ACCEPTING_JOIN,
		OPERATIONAL
	}
	
	private NodeState nodeState;
	
	public Node(NodeDescriptor existingNode, Config conf) throws IOException {
		this.conf=conf;
		nodeState=NodeState.JOINING;
		init();
		log.debug("sending join request to "+existingNode.toString());
		comms.sendNodeMessage(existingNode, new JoinRequestMessage());
		
	}
	public Node(Config conf) throws IOException {
		this.conf=conf;
		nodeState=NodeState.OPERATIONAL;
		init();
	}
	
	private void init() throws IOException {
		comms = new TcpComms(conf);
		comms.open();
		router = new Router(this,conf);
		serviceThread=new ServiceProcessor(this);
		nodeThread=new NodeProcessor(this);
	}
	
	public IComms getComms() {
		return comms;
	}
	
	public HashMap<String,LocalCluster> getLocalClusters(){
		return localClusters;
	}
	
	public NodeState getNodeState() {
		return nodeState;
	}
	
	public void setNodeState(NodeState nodeState) {
		this.nodeState=nodeState;
	}
	
	public NodeProcessor getNodeProcessor() {
		return this.nodeThread;
	}
	
	public Router getRouter(){
		return router;
	}
	
	
}
