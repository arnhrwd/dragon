package dragon.network;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Agent;
import dragon.ComponentError;
import dragon.Config;
import dragon.DragonRequiresClonableException;
import dragon.LocalCluster;
import dragon.metrics.ComponentMetricMap;
import dragon.metrics.Metrics;
import dragon.network.comms.DragonCommsException;
import dragon.network.comms.IComms;
import dragon.network.comms.TcpComms;
import dragon.network.messages.node.JoinRequestNMsg;
import dragon.network.operations.GroupOp;
import dragon.network.operations.ListToposGroupOp;
import dragon.network.operations.Ops;
import dragon.network.operations.TermTopoGroupOp;
import dragon.topology.DragonTopology;
import dragon.topology.base.Component;

/**
 * Node is the main component of the daemon (sometimes used synonymously with
 * daemon), that provides references to all other components. It initializes the
 * Comms, Router, ServiceProcessor, NodeProcessor, Operations, Metrics and
 * maintains a collection of LocalClusters.
 * 
 * @author aaron
 *
 */
public class Node {
	private final static Log log = LogFactory.getLog(Node.class);
	private final IComms comms;

	private final HashMap<String, LocalCluster> localClusters;
	@SuppressWarnings("unused")
	private final ServiceProcessor serviceThread;
	private final NodeProcessor nodeThread;
	private final Ops operationsThread;
	private final Config conf;
	private final Metrics metrics;
	private final Router router;

	public enum NodeState {
		JOINING, JOIN_REQUESTED, ACCEPTING_JOIN, OPERATIONAL
	}

	private NodeState nodeState;

	public Node(Config conf) throws IOException {
		this.conf = conf;
		operationsThread = new Ops(this);
		localClusters = new HashMap<String, LocalCluster>();
		comms = new TcpComms(conf);
		comms.open();
		nodeState = NodeState.JOINING;
		for (NodeDescriptor existingNode : conf.getHosts()) {
			if (!existingNode.equals(comms.getMyNodeDesc())) {
				nodeState = NodeState.JOIN_REQUESTED;
				try {
					comms.sendNodeMsg(existingNode, new JoinRequestNMsg());
				} catch (DragonCommsException e) {
					log.error("failed to join with [" + existingNode + "]: " + e.getMessage());
					nodeState = NodeState.JOINING;
					continue;
				}
				break;
			}
		}
		if (nodeState == NodeState.JOINING) {
			log.warn("did not join with any existing Dragon daemons");
			nodeState = NodeState.OPERATIONAL;
		}
		router = new Router(this, conf);
		serviceThread = new ServiceProcessor(this);
		nodeThread = new NodeProcessor(this);
		if (conf.getDragonMetricsEnabled()) {
			metrics = new Metrics(this);
			metrics.start();
		} else {
			metrics = null;
		}
	}

//	public void test() {
//		for(NodeDescriptor existingNode : conf.getHosts()) {
//			if(!existingNode.equals(comms.getMyNodeDescriptor())) {
//				nodeState=NodeState.JOIN_REQUESTED;
//				RequestReplyOperation rro = new RequestReplyOperation();
//				rro.onStart(()->{
//					try {
//						comms.sendNodeMessage(existingNode, new JoinRequestMessage());
//					} catch (DragonCommsException e) {
//						rro.fail("could not communicate with neighbor ["+existingNode+"]: "+e.getMessage());
//					}
//				});
//				rro.onSuccess(()->{
//						
//				});
//				rro.onFailure((error)->{
//						
//				});
//				operationsThread.register(rro);
//				rro.start();
//			}
//		}
//	}

	public synchronized IComms getComms() {
		return comms;
	}

	public synchronized HashMap<String, LocalCluster> getLocalClusters() {
		return localClusters;
	}

	public synchronized NodeState getNodeState() {
		return nodeState;
	}

	public synchronized void setNodeState(NodeState nodeState) {
		this.nodeState = nodeState;
	}

	public synchronized NodeProcessor getNodeProcessor() {
		return this.nodeThread;
	}

	public synchronized Router getRouter() {
		return router;
	}

	public synchronized Config getConf() {
		return conf;
	}

	public synchronized Ops getOperationsProcessor() {
		return operationsThread;
	}

	/**
	 * Store a byte array, containing the jar file, for a given topology, into the
	 * local file system.
	 * 
	 * @param topologyId  the name of the topology
	 * @param topologyJar the byte array of the jar file
	 * @return true is successfully stored, false otherwise
	 */
	public synchronized boolean storeJarFile(String topologyId, byte[] topologyJar) {
		Path pathname = Paths.get(conf.getJarPath() + "/" + comms.getMyNodeDesc(), topologyId);
		File f = new File(pathname.getParent().toString());
		f.mkdirs();
		try (FileOutputStream fos = new FileOutputStream(pathname.toString())) {
			fos.write(topologyJar);
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		log.fatal("failed to store topology jar file for [" + topologyId + "]");
		return false;
	}

	/**
	 * Add a jar file for a given topology to the JVM classpath.
	 * 
	 * @param topologyId the name of the topology to load
	 * @return true if loaded successfully, false otherwise
	 */
	public synchronized boolean loadJarFile(String topologyId) {
		Path pathname = Paths.get(conf.getJarPath() + "/" + comms.getMyNodeDesc(), topologyId);
		try {
			Agent.addToClassPath(new File(pathname.toString()));
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		log.fatal("failed to add topology jar file to the classpath [" + topologyId + "]");
		return false;
	}

	/**
	 * Read a topology jar file into a byte array.
	 * 
	 * @param topologyId the name of the topology to load
	 * @return null if not loaded successfully, otherwise a byte array
	 */
	public synchronized byte[] readJarFile(String topologyId) {
		Path pathname = Paths.get(conf.getJarPath() + "/" + comms.getMyNodeDesc(), topologyId);

		File file = new File(pathname.toString());
		try {
			return Files.readAllBytes(file.toPath());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

	}

	/**
	 * Allocate a LocalCluster for the given topology. The Conf given to the local
	 * cluster is first formed from this daemon's conf and then over written by the
	 * supplied conf.
	 * 
	 * @param topologyId the name of the topology
	 * @param conf       the specific conf parameters for the topology
	 * @param topology   the topology information
	 * @param start      whether to start the topology immediately (local mode) or
	 *                   not
	 * @throws DragonRequiresClonableException if the topology contains components
	 *                                         that are not cloneable
	 * @throws DragonTopologyException         if the topology already exists
	 */
	public synchronized void prepareTopology(String topologyId, Config conf, DragonTopology topology, boolean start)
			throws DragonRequiresClonableException, DragonTopologyException {
		if (localClusters.containsKey(topologyId))
			throw new DragonTopologyException("topology already exists: " + topologyId);
		LocalCluster cluster = new LocalCluster(this);
		Config lconf = new Config();
		lconf.putAll(this.conf);
		lconf.putAll(conf);
		cluster.submitTopology(topologyId, lconf, topology, start);
		getRouter().submitTopology(topologyId, topology);
		getLocalClusters().put(topologyId, cluster);
	}

	/**
	 * Starts a local topology, by scheduling the spouts to run.
	 * 
	 * @param topologyId the name of the topology to start
	 * @throws DragonTopologyException if the topology does not exist
	 */
	public synchronized void startTopology(String topologyId) throws DragonTopologyException {
		if (!localClusters.containsKey(topologyId))
			throw new DragonTopologyException("topology does not exist: " + topologyId);
		localClusters.get(topologyId).openAll();
	}

	/**
	 * Terminates a topology, waiting for all of the outstanding tuples to be
	 * processed first.
	 * 
	 * @param topologyId the name of the topology to terminate
	 * @param go         the group operation to respond to when the topology
	 *                   finishes terminating
	 * @throws DragonTopologyException if the topology does not exist
	 */
	public synchronized void terminateTopology(String topologyId, GroupOp go) throws DragonTopologyException {
		if (!localClusters.containsKey(topologyId))
			throw new DragonTopologyException("topology does not exist: " + topologyId);
		LocalCluster localCluster = getLocalClusters().get(topologyId);
		localCluster.setGroupOperation(go);
		localCluster.setShouldTerminate();
	}

	/**
	 * Return metrics for the given topology.
	 * 
	 * @param topologyId the name of the topology
	 * @return the metrics for the topology, or null if metrics are not available
	 */
	public synchronized ComponentMetricMap getMetrics(String topologyId) {
		if (metrics != null) {
			return metrics.getMetrics(topologyId);
		} else
			return null;
	}

	/**
	 * Signal to the group operation that the local cluster has finished
	 * terminating.
	 * 
	 * @param ttgo the group operation to respond to
	 */
	public synchronized void localClusterTerminated(TermTopoGroupOp ttgo) {
		ttgo.sendSuccess(comms);
	}

	/**
	 * Remove the name topology from the collection of local clusters. The topology
	 * must be completely terminated, i.e. over all daemons, before removing it. A
	 * garbage collection is called after removal. Router queues for the topology
	 * are also removed.
	 * 
	 * @param topologyId the name of the topology to remove
	 * @throws DragonTopologyException
	 */
	public synchronized void removeTopo(String topologyId) throws DragonTopologyException {
		if (!localClusters.containsKey(topologyId))
			throw new DragonTopologyException("topology does not exist: " + topologyId);
		router.terminateTopology(topologyId, localClusters.get(topologyId).getTopology());
		localClusters.remove(topologyId);
		System.gc();
	}

	/**
	 * Autonomously called by a local cluster in the case of topology failure, that
	 * signals other daemons to halt the topology.
	 * 
	 * @param topologyId the name of the topology that has failed
	 */
	public synchronized void signalHaltTopology(String topologyId) {
		operationsThread.newHaltTopoGroupOp(topologyId, (op) -> {
			log.warn("topology was halted due to too many errors");
		}, (op, error) -> {
			log.fatal(error);
		}).onRunning((op) -> {
			try {
				haltTopology(topologyId);
			} catch (DragonTopologyException e) {
				op.fail(e.getMessage());
			}
		});
	}

	/**
	 * Halt the local topology by suspending all of its threads.
	 * 
	 * @param topologyId the name of the topology to halt
	 * @throws DragonTopologyException if the topology does not exist
	 */
	public synchronized void haltTopology(String topologyId) throws DragonTopologyException {
		if (!localClusters.containsKey(topologyId))
			throw new DragonTopologyException("topology does not exist: " + topologyId);
		localClusters.get(topologyId).haltTopology();
	}

	/**
	 * Gather information about all of the local topologies running on this node.
	 * 
	 * @param ltgo the group op to respond to
	 */
	public synchronized void listTopologies(ListToposGroupOp ltgo) {
		HashMap<String, String> state = new HashMap<String, String>();
		HashMap<String, HashMap<String, ArrayList<ComponentError>>> errors = new HashMap<String, HashMap<String, ArrayList<ComponentError>>>();
		for (String topologyId : localClusters.keySet()) {
			state.put(topologyId, localClusters.get(topologyId).getState().name());
			errors.put(topologyId, new HashMap<String, ArrayList<ComponentError>>());
			for (Component component : localClusters.get(topologyId).getComponentErrors().keySet()) {
				String name = component.getComponentId() + ":" + component.getTaskId();
				errors.get(topologyId).put(name, localClusters.get(topologyId).getComponentErrors().get(component));
			}
		}

		/*
		 * Store the data into the holding variables prior to sending the response.
		 */
		ltgo.state = state;
		ltgo.errors = errors;

		ltgo.sendSuccess(comms);
	}

	/**
	 * Resume a halted topology, by signalling all the threads that they can
	 * continue.
	 * 
	 * @param topologyId the name of the topology to resume
	 * @throws DragonTopologyException if the topology does not exist
	 */
	public synchronized void resumeTopology(String topologyId) throws DragonTopologyException {
		if (!localClusters.containsKey(topologyId))
			throw new DragonTopologyException("topology does not exist: " + topologyId);
		localClusters.get(topologyId).resumeTopology();
	}

}
