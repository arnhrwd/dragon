package dragon.network;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.LocalCluster;
import dragon.network.messages.node.JoinRequest;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.service.RunTopology;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TopologyExists;
import dragon.network.messages.service.TopologySubmitted;

public class Node {
	private static Log log = LogFactory.getLog(Node.class);
	private IComms comms;
	private ExecutorService networkExecutorService;
	private HashMap<String,LocalCluster> localClusters;
	private Thread serviceThread;
	private Thread nodeThread;
	private boolean shouldTerminate=false;
	private HashSet<NodeMessage> pendingJoinRequests;
	
	private enum NodeState {
		JOINING,
		JOIN_REQUESTED,
		OPERATIONAL
	}
	
	private NodeState nodeState;
	
	public Node(NodeDescriptor existingNode) {
		nodeState=NodeState.JOINING;
		init();
		comms.sendNodeMessage(existingNode, new JoinRequest());
		
	}
	public Node() {
		nodeState=NodeState.OPERATIONAL;
		init();
	}
	
	private void init() {
		pendingJoinRequests = new HashSet<NodeMessage>();
		comms = new TcpComms();
		log.debug("opening comms");
		comms.open();
		serviceThread=new Thread(){
			public void run(){
				while(!shouldTerminate){
					ServiceMessage command = comms.receiveServiceMessage();
					switch(command.getType()){
					case RUN_TOPOLOGY:
						RunTopology scommand = (RunTopology) command;
						if(localClusters.containsKey(scommand.topologyName)){
							comms.sendServiceMessage(new TopologyExists(scommand.topologyName));
						} else {
							LocalCluster cluster=new LocalCluster();
							cluster.submitTopology(scommand.topologyName, scommand.conf, scommand.dragonTopology);
							localClusters.put(scommand.topologyName, cluster);
							comms.sendServiceMessage(new TopologySubmitted(scommand.topologyName));
						}
						break;
					default:
					}
				}
			}
		};
		log.debug("starting service thread");
		serviceThread.start();
		nodeThread=new Thread() {
			public void run() {
				while(!shouldTerminate) {
					NodeMessage message = comms.receiveNodeMessage();
					switch(message.getType()) {
					case JOIN_REQUEST:
						if(nodeState!=NodeState.OPERATIONAL) {
							pendingJoinRequests.add(message);
						} else {
							
						}
						break;
					}
				}
			}
		};
		log.debug("starting node thread");
		nodeThread.start();
	}
	
	
}
