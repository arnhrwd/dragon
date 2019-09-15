package dragon.network;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.DragonSubmitter;
import dragon.LocalCluster;
import dragon.Run;
import dragon.metrics.ComponentMetricMap;
import dragon.metrics.Metrics;
import dragon.network.comms.IComms;
import dragon.network.comms.TcpComms;
import dragon.network.messages.node.JoinRequestMessage;
import dragon.network.messages.node.StartTopologyMessage;
import dragon.network.messages.service.TopologyTerminatedMessage;



public class Node {
	private static Log log = LogFactory.getLog(Node.class);
	private IComms comms;

	private HashMap<String,LocalCluster> localClusters;
	private ServiceProcessor serviceThread;
	private NodeProcessor nodeThread;
	private boolean shouldTerminate=false;
	private Config conf;
	private Metrics metrics;
	
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
		nodeState=NodeState.JOIN_REQUESTED;
		init();
		comms.sendNodeMessage(existingNode, new JoinRequestMessage());
		
	}
	public Node(Config conf) throws IOException {
		this.conf=conf;
		nodeState=NodeState.OPERATIONAL;
		init();
	}
	
	private void init() throws IOException {
		localClusters = new HashMap<String,LocalCluster>();
		startupTopology = new HashMap<String,HashSet<NodeDescriptor>>();
		comms = new TcpComms(conf);
		comms.open();
		router = new Router(this,conf);
		serviceThread=new ServiceProcessor(this);
		nodeThread=new NodeProcessor(this);
		if((Boolean)conf.get(Config.DRAGON_METRICS_ENABLED)){
			metrics = new Metrics(this);
			metrics.start();
		}
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
		Path pathname = Paths.get(conf.getJarDir()+"/"+comms.getMyNodeDescriptor(),topologyName);
		File f = new File(pathname.getParent().toString());
		f.mkdirs();
		try (FileOutputStream fos = new FileOutputStream(pathname.toString())) {
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
		Path pathname = Paths.get(conf.getJarDir()+"/"+comms.getMyNodeDescriptor(),topologyName);
		try {
			Run.addClassPath(pathname.toString());
			return true;
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}
		log.error("failed to add topology jar file to the classpath ["+topologyName+"]");
		return false;
	}
	
	public byte[] readJarFile(String topologyName) {
		Path pathname = Paths.get(conf.getJarDir()+"/"+comms.getMyNodeDescriptor(),topologyName);
		
		File file = new File(pathname.toString());
		try {
			return Files.readAllBytes(file.toPath());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
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
	
	public ComponentMetricMap getMetrics(String topologyId){
		if(metrics!=null){
			return metrics.getMetrics(topologyId);
		} else return null;
	}
	
	public void localClusterTerminated(String topologyId, String messageId) {
		localClusters.remove(topologyId);
		if(messageId!=null) {
			TopologyTerminatedMessage ttm = new TopologyTerminatedMessage(topologyId);
			ttm.setMessageId(messageId);
			comms.sendServiceMessage(ttm);
		}
	}
	
}