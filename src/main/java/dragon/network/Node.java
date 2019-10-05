package dragon.network;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Agent;
import dragon.Config;
import dragon.DragonRequiresClonableException;
import dragon.LocalCluster;
import dragon.metrics.ComponentMetricMap;
import dragon.metrics.Metrics;
import dragon.network.comms.DragonCommsException;
import dragon.network.comms.IComms;
import dragon.network.comms.TcpComms;
import dragon.network.messages.node.HaltTopologyMessage;
import dragon.network.messages.node.JoinRequestMessage;
import dragon.network.operations.GroupOperation;
import dragon.network.operations.TerminateTopologyGroupOperation;
import dragon.topology.DragonTopology;

public class Node {
	private static Log log = LogFactory.getLog(Node.class);
	private IComms comms;

	private HashMap<String,LocalCluster> localClusters;
	@SuppressWarnings("unused")
	private ServiceProcessor serviceThread;
	private NodeProcessor nodeThread;
	private Config conf;
	private Metrics metrics;
	private long groupOperationCounter=0;
	private HashMap<Long,GroupOperation> groupOperations;
	private Router router;
	
	public enum NodeState {
		JOINING,
		JOIN_REQUESTED,
		ACCEPTING_JOIN,
		OPERATIONAL
	}
	
	private NodeState nodeState;
	
	public Node(Config conf) throws IOException {
		this.conf=conf;
		groupOperations=new HashMap<Long,GroupOperation>();
		localClusters = new HashMap<String,LocalCluster>();
		comms = new TcpComms(conf);
		comms.open();
		nodeState=NodeState.JOINING;
		for(NodeDescriptor existingNode : conf.getHosts()) {
			if(!existingNode.equals(comms.getMyNodeDescriptor())) {
				nodeState=NodeState.JOIN_REQUESTED;
				try {
					comms.sendNodeMessage(existingNode, new JoinRequestMessage());
				} catch (DragonCommsException e) {
					log.error("failed to join with ["+existingNode+"]: "+e.getMessage());
					nodeState=NodeState.JOINING;
					continue;
				}
				break;
			}
		}
		if(nodeState==NodeState.JOINING) {
			log.warn("did not join with any existing Dragon daemons");
			nodeState=NodeState.OPERATIONAL;
		}
		router = new Router(this,conf);
		serviceThread=new ServiceProcessor(this);
		nodeThread=new NodeProcessor(this);
		if(conf.getDragonMetricsEnabled()){
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
		Path pathname = Paths.get(conf.getJarPath()+"/"+comms.getMyNodeDescriptor(),topologyName);
		File f = new File(pathname.getParent().toString());
		f.mkdirs();
		try (FileOutputStream fos = new FileOutputStream(pathname.toString())) {
		   fos.write(topologyJar);
		   return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		log.fatal("failed to store topology jar file for ["+topologyName+"]");
		return false;
	}
	
	public boolean loadJarFile(String topologyName) {
		Path pathname = Paths.get(conf.getJarPath()+"/"+comms.getMyNodeDescriptor(),topologyName);
		try {
			Agent.addToClassPath(new File(pathname.toString()));
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		log.fatal("failed to add topology jar file to the classpath ["+topologyName+"]");
		return false;
	}
	
	public byte[] readJarFile(String topologyName) {
		Path pathname = Paths.get(conf.getJarPath()+"/"+comms.getMyNodeDescriptor(),topologyName);
		
		File file = new File(pathname.toString());
		try {
			return Files.readAllBytes(file.toPath());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
	}
	
	public synchronized void prepareTopology(String topologyId, Config conf, DragonTopology topology, boolean start) throws DragonRequiresClonableException {
		LocalCluster cluster=new LocalCluster(this);
		Config lconf = new Config();
		lconf.putAll(this.conf);
		lconf.putAll(conf);
		cluster.submitTopology(topologyId, lconf, topology, start);
		getRouter().submitTopology(topologyId,topology);
		getLocalClusters().put(topologyId, cluster);
	}
	
	public void startTopology(String topologyId) {
		localClusters.get(topologyId).openAll();
	}
	
	public synchronized void stopTopology(String topologyId,GroupOperation go) {
		LocalCluster localCluster = getLocalClusters().get(topologyId);
		localCluster.setGroupOperation(go);
		localCluster.setShouldTerminate();
	}
	
	public ComponentMetricMap getMetrics(String topologyId){
		if(metrics!=null){
			return metrics.getMetrics(topologyId);
		} else return null;
	}
	
	public synchronized void localClusterTerminated(String topologyId, TerminateTopologyGroupOperation ttgo) {
		router.terminateTopology(topologyId, localClusters.get(topologyId).getTopology());
		localClusters.remove(topologyId);
		System.gc();
		ttgo.sendSuccess(comms);
	}

	public void register(GroupOperation groupOperation) {
		synchronized(groupOperations) {
			groupOperation.init(comms.getMyNodeDescriptor(),groupOperationCounter);
			groupOperations.put(groupOperationCounter, groupOperation);
			groupOperationCounter++;
		}
	}
	
	public GroupOperation getGroupOperation(Long id) {
		synchronized(groupOperations) {
			return groupOperations.get(id);
		}
	}
	
	public void removeGroupOperation(Long id) {
		synchronized(groupOperations) {
			groupOperations.remove(id);
		}
	}
	
	public void signalHaltTopology(String topologyName) {
		for(NodeDescriptor desc : localClusters.get(topologyName).getTopology().getReverseEmbedding().keySet()) {
			if(!desc.equals(getComms().getMyNodeDescriptor())) {
				try {
					getComms().sendNodeMessage(desc, new HaltTopologyMessage(topologyName));
				} catch (DragonCommsException e) {
					log.error("could not signal halt topology");
				}
			}
		}
		haltTopology(topologyName);
	}

	public synchronized void haltTopology(String topologyName) {
		if(localClusters.containsKey(topologyName)) {
			localClusters.get(topologyName).haltTopology();
		} else {
			log.error("cannot halt topology as it does not exist ["+topologyName+"]");
		}
	}
}
