package dragon.network;


import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.LocalCluster;
import dragon.network.Node.NodeState;
import dragon.network.messages.node.AcceptingJoinMessage;
import dragon.network.messages.node.ContextUpdateMessage;
import dragon.network.messages.node.JarReadyMessage;
import dragon.network.messages.node.JoinCompleteMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PrepareJarErrorMessage;
import dragon.network.messages.node.PrepareJarMessage;
import dragon.network.messages.node.PrepareTopologyMessage;
import dragon.network.messages.node.StartTopologyMessage;
import dragon.network.messages.node.TopologyReadyMessage;
import dragon.network.messages.service.RunTopologyErrorMessage;
import dragon.network.messages.service.TopologyRunningMessage;

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
			case PREPARE_JAR:
				{
					PrepareJarMessage pjf = (PrepareJarMessage) message;
					if(!node.storeJarFile(pjf.topologyName,pjf.topologyJar)) {
						PrepareJarErrorMessage r = new PrepareJarErrorMessage(pjf.topologyName,"could not store the topology jar");
						r.setMessageId(message.getMessageId());
						node.getComms().sendNodeMessage(pjf.getSender(), r);
						continue;
					} else if(!node.loadJarFile(pjf.topologyName)) {
						PrepareJarErrorMessage r = new PrepareJarErrorMessage(pjf.topologyName,"could not load the topology jar");
						r.setMessageId(message.getMessageId());
						node.getComms().sendNodeMessage(pjf.getSender(), r);
						continue;
					}
					JarReadyMessage jrm = new JarReadyMessage(pjf.topologyName);
					jrm.setMessageId(message.getMessageId());
					node.getComms().sendNodeMessage(message.getSender(), jrm);
				}
				break;
			
			case JAR_READY:
				{
					JarReadyMessage jrm = (JarReadyMessage) message;
					NodeMessage response = new PrepareTopologyMessage(jrm.topologyId,
							node.getLocalClusters().get(jrm.topologyId).getConf(),
							node.getLocalClusters().get(jrm.topologyId).getTopology());
					response.setMessageId(message.getMessageId());
					node.getComms().sendNodeMessage(message.getSender(), response);
				}
				break;
			case PREPARE_TOPOLOGY:
				PrepareTopologyMessage pt = (PrepareTopologyMessage) message;
				{
					LocalCluster cluster=new LocalCluster(node);
					cluster.submitTopology(pt.topologyName, pt.conf, pt.topology, false);
					node.getRouter().submitTopology(pt.topologyName,pt.topology);
					node.getLocalClusters().put(pt.topologyName, cluster);
					TopologyReadyMessage r = new TopologyReadyMessage(pt.topologyName);
					r.setMessageId(message.getMessageId());
					node.getComms().sendNodeMessage(pt.getSender(), r);
				}
				break;
			case TOPOLOGY_READY:
				TopologyReadyMessage tr = (TopologyReadyMessage) message;
				if(node.checkStartupTopology(tr.getSender(),tr.topologyId)) {
					TopologyRunningMessage r = new TopologyRunningMessage(tr.topologyId);
					r.setMessageId(tr.getMessageId());
					node.getComms().sendServiceMessage(r);
				}
				break;
			case START_TOPOLOGY:
				StartTopologyMessage st = (StartTopologyMessage) message;
				node.startTopology(st.topologyId);
				break;
			case PREPARE_JAR_ERROR:
				PrepareJarErrorMessage pf = (PrepareJarErrorMessage) message;
				node.removeStartupTopology(pf.topologyId);
				RunTopologyErrorMessage r = new RunTopologyErrorMessage(pf.topologyId,pf.error);
				r.setMessageId(pf.getMessageId());
				node.getComms().sendServiceMessage(r);
				break;
			case STOP_TOPOLOGY:
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
