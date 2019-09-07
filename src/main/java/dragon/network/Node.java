package dragon.network;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.LocalCluster;
import dragon.Run;
import dragon.network.comms.IComms;
import dragon.network.comms.TcpComms;
import dragon.network.messages.node.JoinRequestMessage;
import dragon.network.messages.node.StartTopologyMessage;



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
	
	private HashMap<String,HashSet<NodeDescriptor>> startupTopology;
	
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
		startupTopology = new HashMap<String,HashSet<NodeDescriptor>>();
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
	
	public Config getConf() {
		return conf;
	}
	
	public boolean storeJarFile(String topologyName, byte[] topologyJar) {
		String pathname = conf.getJarDir()+"/"+topologyName;
		File f = new File(pathname);
		f.mkdirs();
		try (FileOutputStream fos = new FileOutputStream(pathname)) {
		   fos.write(topologyJar);
		   return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		log.error("failed to store topology jar file for ["+topologyName+"]");
		return false;
	}
	
	public boolean loadJarFile(String topologyName) {
		String pathname = conf.getJarDir()+"/"+topologyName;
		try {
			Run.addClassPath(pathname);
			return true;
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}
		log.error("failed to add topology jar file to the classpath ["+topologyName+"]");
		return false;
	}
	
	public void createStartupTopology(String topologyId) {
		startupTopology.put(topologyId,new HashSet<NodeDescriptor>());
		startupTopology.get(topologyId).add(comms.getMyNodeDescriptor());
	}
	public boolean checkStartupTopology(NodeDescriptor sender, String topologyId) {
		startupTopology.get(topologyId).add(sender);
		if(startupTopology.get(topologyId).size()==localClusters.get(topologyId).getTopology().getReverseEmbedding().size()) {
			localClusters.get(topologyId).openAll();
			for(NodeDescriptor desc : startupTopology.get(topologyId)) {
				if(!desc.equals(comms.getMyNodeDescriptor())) {
					comms.sendNodeMessage(desc, new StartTopologyMessage(topologyId));
				}
			}
			startupTopology.remove(topologyId);
			return true;
		}
		return false;
	}
	public void startTopology(String topologyId) {
		localClusters.get(topologyId).openAll();
	}
	
	public void removeStartupTopology(String topologyId) {
		startupTopology.remove(topologyId);
	}
	
}