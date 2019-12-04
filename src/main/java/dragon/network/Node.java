package dragon.network;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
import dragon.network.messages.node.AcceptingJoinNMsg;
import dragon.network.messages.node.ContextUpdateNMsg;
import dragon.network.messages.node.JoinCompleteNMsg;
import dragon.network.operations.GroupOp;
import dragon.network.operations.JoinGroupOp;
import dragon.network.operations.ListToposGroupOp;
import dragon.network.operations.Ops;
import dragon.network.operations.TermTopoGroupOp;
import dragon.topology.DragonTopology;
import dragon.topology.base.Component;

/**
 * Node is the main component of the daemon (sometimes used synonymously with
 * daemon), that provides references to all other components. It initializes the
 * Comms, Router, ServiceProcessor, NodeProcessor, Operations, Metrics and
 * maintains a collection of LocalClusters. Methods provided by Node may be called
 * by the processor threads and by the local clusters, and therefore need to be
 * synchronized.
 * 
 * @author aaron
 *
 */
public class Node {
	private final static Log log = LogFactory.getLog(Node.class);
	
	/**
	 * The communications layer that this node is using.
	 */
	private final IComms comms;

	/**
	 * A map from topologyId to LocalCluster for each topology that is
	 * currently allocated on this node.
	 */
	private final HashMap<String, LocalCluster> localClusters;
	
	/**
	 * The service processor thread.
	 */
	@SuppressWarnings("unused")
	private final ServiceProcessor serviceThread;
	
	/**
	 * The node processor thread.
	 */
	private final NodeProcessor nodeThread;
	
	/**
	 * The operations processor thread.
	 */
	private final Ops operationsThread;
	
	/**
	 * The configuration loaded by this node at startup.
	 */
	private final Config conf;
	
	/**
	 * The metrics thread.
	 */
	private final Metrics metricsThread;
	
	/**
	 * The router for this node.
	 */
	private final Router router;

	/**
	 * The possible states that the node is in.
	 * <li>{@link #JOINING}</li>
	 * <li>{@link #JOIN_REQUESTED}</li>
	 * <li>{@link #ACCEPTING_JOIN}</li>
	 * <li>{@link #OPERATIONAL}</li>
	 */
	public enum NodeState {
		/**
		 * The node is currently starting up and is determining whether
		 * to join or not to an existing node.
		 */
		JOINING, 
		
		/**
		 * The node has sent a join request message and is waiting for
		 * an accepted join message in response.
		 */
		JOIN_REQUESTED, 
		
		/**
		 * The node has accepted a join request from another node and is
		 * waiting for a join complete message.
		 */
		ACCEPTING_JOIN, 
		
		/**
		 * The node is available to process general messages. 
		 */
		OPERATIONAL
	}

	/**
	 * The state that this node is in.
	 */
	private NodeState nodeState;
	
	/**
	 * Arguments used to start this JVM
	 */
	private List<String> jvmArgs;

	/**
	 * Initialize the node, will initiate a join request if possible to
	 * join to existing daemons.
	 * @param conf provides the configuration to use
	 * @throws IOException
	 */
	public Node(Config conf) throws IOException {

		RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
		jvmArgs = bean.getInputArguments();


		// java -javaagent:dragon.jar -jar dragon.jar -d
		// -javaagent:dragon.jar
		for (int i = 0; i < jvmArgs.size(); i++) {
			log.debug(jvmArgs.get(i));
		}
		// -classpath dragon.jar:dragon.jar
		log.debug(" -classpath " + System.getProperty("java.class.path"));
		// dragon.jar -d
		log.debug(" " + System.getProperty("sun.java.command"));

		this.conf = conf;
		operationsThread = new Ops(this);
		localClusters = new HashMap<String, LocalCluster>();
		comms = new TcpComms(conf);
		comms.open();
		setNodeState(NodeState.JOINING);
		router = new Router(this, conf);
		serviceThread = new ServiceProcessor(this);
		nodeThread = new NodeProcessor(this);
		if (conf.getDragonMetricsEnabled()) {
			metricsThread = new Metrics(this);
			metricsThread.start();
		} else {
			metricsThread = null;
		}
		
		final ArrayList<NodeDescriptor> hosts = conf.getHosts();
		sendJoinRequest(hosts);
	}
	
	/**
	 * Send a join request, progressively trying all hosts in the list until
	 * one is found that is successful.
	 * @param hosts the list of hosts to try to join to
	 */
	private void sendJoinRequest(final ArrayList<NodeDescriptor> hosts) {
		if(hosts.isEmpty()) {
			log.warn("did not join with any existing Dragon daemons");
			setNodeState(NodeState.OPERATIONAL);
			return;
		}
		NodeDescriptor desc = hosts.remove(0);
		if(desc.equals(comms.getMyNodeDesc())) {
			sendJoinRequest(hosts);
			return;
		} else {
			Ops.inst().newJoinGroupOp(desc, (op)->{
				log.info("joined to "+desc);
				JoinGroupOp jgo = (JoinGroupOp) op;
				AcceptingJoinNMsg aj = (AcceptingJoinNMsg) jgo.getReceived().get(0);
				nodeThread.setNextNode(aj.nextNode);
				
				try {
					comms.sendNodeMsg(aj.getSender(), new JoinCompleteNMsg());
				} catch (DragonCommsException e) {
					log.error("could not complete join with ["+aj.getSender());
					// TODO: possibly signal that the node has failed
				}
				nodeThread.contextPutAll(aj.context);
				for(NodeDescriptor descriptor : nodeThread.getContext().values()) {
					if(!descriptor.equals(comms.getMyNodeDesc())) {
						try {
							comms.sendNodeMsg(descriptor, new ContextUpdateNMsg(nodeThread.getContext()));
						} catch (DragonCommsException e) {
							log.error("could not send context update to ["+descriptor+"]");
							// TODO: possibly signal that the node has failed
						}
					}
				}
				
				setNodeState(NodeState.OPERATIONAL);
			}, (op,error)->{
				setNodeState(NodeState.JOINING);
				log.warn("error while joining: "+error);
				sendJoinRequest(hosts);
			}).onStart((op)->{
				setNodeState(NodeState.JOIN_REQUESTED);
			});
		}
	}

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
		log.info("state is now ["+nodeState+"]");
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

	public synchronized Ops getOpsProcessor() {
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
		if (metricsThread != null) {
			return metricsThread.getMetrics(topologyId);
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
	
	public synchronized void allocatePartition(String partitionId,int daemons) {
		
		
		List<String> pbArgs = new ArrayList<String>();
		pbArgs.add(conf.getDragonJavaBin());
		pbArgs.addAll(jvmArgs);
		pbArgs.add("-classpath");
		pbArgs.add(System.getProperty("java.class.path"));
		pbArgs.add("-jar");
		pbArgs.add("dragon.jar");
		pbArgs.add("-d");
		
		ProcessBuilder pb = new ProcessBuilder(pbArgs);

		try {
			Process p = pb.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
