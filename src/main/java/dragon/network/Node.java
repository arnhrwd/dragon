package dragon.network;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.ComponentError;
import dragon.Config;
import dragon.DragonInvalidStateException;
import dragon.DragonRequiresClonableException;
import dragon.LocalCluster;
import dragon.metrics.ComponentMetricMap;
import dragon.metrics.Metrics;
import dragon.metrics.Sample;
import dragon.network.comms.DragonCommsException;
import dragon.network.comms.IComms;
import dragon.network.comms.TcpComms;
import dragon.network.messages.node.context.ContextUpdateNMsg;
import dragon.network.messages.node.fault.NodeFaultNMsg;
import dragon.network.messages.node.fault.RipNMsg;
import dragon.network.messages.node.fault.TopoFaultNMsg;
import dragon.network.operations.DragonInvalidContext;
import dragon.network.operations.GroupOp;
import dragon.network.operations.ListToposGroupOp;
import dragon.network.operations.Ops;
import dragon.network.operations.TermTopoGroupOp;
import dragon.process.ProcessManager;
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
	private final static Logger log = LogManager.getLogger(Node.class);
	
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
	private final ServiceMsgProcessor serviceThread;
	
	/**
	 * The node processor thread.
	 */
	private final NodeMsgProcessor nodeThread;
	
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
	 * The parent if one exists.
	 */
	NodeDescriptor parentDesc;
	
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
		OPERATIONAL,
		
		/**
		 * The node is going down.
		 */
		TERMINATING
		
	}

	/**
	 * The state that this node is in.
	 */
	private NodeState nodeState;
	
	/**
	 * Secondary daemons started by this primary daemon
	 */
	private HashMap<NodeDescriptor,Process> childProcesses;	
	
	/**
	 * Process manager
	 */
	private ProcessManager processManager;
	
	/**
	 * 
	 */
	private final ReentrantLock operationsLock = new ReentrantLock();

	
	/**
	 * General timer
	 */
	private final Timer timer;
	
	/**
	 * A separate class loader is used for each topology.
	 */
	public HashMap<String,ClassLoader> pluginLoaders;
	
	/**
	 * The class names available for each topology.
	 */
	public HashMap<String,HashSet<String>> pluginClasses;
	
	/**
	 * Singleton reference
	 */
	private static Node me;
	
	public static Node inst() {
		return me;
	}
	
	/**
	 * Initialize the node, will initiate a join request if possible to
	 * join to existing daemons, unless it is the first listed host in the
	 * host list, in which case it will just wait for others to join to it.
	 * @param conf provides the configuration to use
	 * @throws IOException
	 */
	public Node(Config conf) throws IOException {
		me=this;
		this.conf = conf;
		this.parentDesc = conf.getDragonNetworkParentDescriptor();
		writePid();
		
		/*
		 * Allocate class loaders and name spaces for topologies. 
		 */
		pluginLoaders = new HashMap<>();
		pluginClasses = new HashMap<>();
		
		/*
		 * Allocate the local clusters map.
		 */
		localClusters = new HashMap<>();
		
		/*
		 * Allocate the child processes map.
		 */
		
		childProcesses=new HashMap<>();
		
		/*
		 * Allocate the time used for time outs throughout
		 * the system.
		 */
		timer=new Timer("timer");
		
		/*
		 * Start up a process manager.
		 */
		processManager = new ProcessManager(conf);
		
		/*
		 * Start up the operations manager.
		 */
		operationsThread = new Ops();
		
		/*
		 * This node is considering joining to other nodes.
		 */
		//setNodeState(NodeState.JOINING);
		setNodeState(NodeState.OPERATIONAL);
		
		/*
		 * Open the communications layer.
		 */
		comms = new TcpComms(conf);
		comms.open();
		
		/*
		 * Start up the node message processor for node messages.
		 */
		nodeThread = new NodeMsgProcessor();
		
		/*
		 * Start up the router for topology data.
		 */
		router = new Router(conf,comms,localClusters);
		
		/*
		 * Start up the service message processor for client commands.
		 */
		serviceThread = new ServiceMsgProcessor();
		
		/*
		 * If metrics is desired, startup a metrics monitor.
		 */
		if (conf.getDragonMetricsEnabled()) {
			metricsThread = new Metrics(conf,localClusters,comms.getMyNodeDesc());
		} else {
			metricsThread=null;
		}
		
		/*
		 * Get a list of hosts from the configuration file to connect with.
		 */
		final ArrayList<NodeDescriptor> hosts = conf.getHosts();
		if(hosts.size()>0 && !hosts.get(0).equals(comms.getMyNodeDesc())) {
			sendInitialContextUpdate(hosts);
		} 
	}
	
	/**
	 * Send a context update, progressively trying all hosts in the list.
	 * @param hosts the list of hosts to try to connect to
	 */
	private void sendInitialContextUpdate(final ArrayList<NodeDescriptor> hosts) {
		if(hosts.isEmpty()) {
			return;
		}
		NodeDescriptor desc = hosts.remove(0);
		while(nodeThread.getAliveContext().containsKey(desc.toString())) {
			if(hosts.isEmpty())return;
			desc = hosts.remove(0);
		}
		final NodeDescriptor dest = desc;
		Ops.inst().newOp((op)->{
			try {
				Node.inst().getComms().sendNodeMsg(dest, new ContextUpdateNMsg(nodeThread.getAliveContext()));
			} catch (DragonCommsException e) {
				nodeThread.setDead(dest);
				op.fail("["+dest+"] is not reachable");
			}
		}, (op)->{
			op.success();
		}, (op)->{
			log.info("sent context update to ["+dest+"]");
			sendInitialContextUpdate(hosts);
		}, (op,msg)->{
			log.warn(msg);
			sendInitialContextUpdate(hosts);
		});
		
	}
	
	/**
	 * Utility function to write the pid to a file.
	 * @throws IOException 
	 */
	private void writePid() throws IOException {
		Long pid = ProcessHandle.current().pid();
		log.debug("pid = "+pid);
		log.debug("writing pid to ["+conf.getDragonDataDir()+"/dragon-"+conf.getDragonNetworkLocalDataPort()+".pid]");
		File fout = new File(conf.getDragonDataDir()+"/dragon-"+conf.getDragonNetworkLocalDataPort()+".pid");
		File datadir = new File(conf.getDragonDataDir());
		datadir.mkdirs();
		FileOutputStream fos = new FileOutputStream(fout);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		bw.write(pid.toString());
		bw.newLine();
		bw.close();
		if(ProcessHandle.current().parent().isPresent()) {
			long parent_pid = ProcessHandle.current().parent().get().pid();
			log.debug("parent pid = "+parent_pid);
		}
	}

	/**
	 *  
	 * @return the comms layer for this node.
	 */
	public synchronized IComms getComms() {
		return comms;
	}
	
	/**
	 * 
	 * @return the process manager for this node.
	 */
	public synchronized ProcessManager getProcessManager() {
		return processManager;
	}

	/**
	 *  
	 * @return the local cluster map for this node.
	 */
	public synchronized HashMap<String, LocalCluster> getLocalClusters() {
		return localClusters;
	}

	/**
	 * 
	 * @return the node state for this node.
	 */
	public synchronized NodeState getNodeState() {
		return nodeState;
	}

	/**
	 * Set the node state for this node.
	 * @param nodeState
	 */
	public synchronized void setNodeState(NodeState nodeState) {
		log.info("state is now ["+nodeState+"]");
		this.nodeState = nodeState;
	}
	
	public synchronized ReentrantLock getOperationsLock() {
		return operationsLock;
	}

	/**
	 *  
	 * @return the node processor for this node.
	 */
	public synchronized NodeMsgProcessor getNodeProcessor() {
		return this.nodeThread;
	}

	/**
	 * 
	 * @return he router for this node.
	 */
	public synchronized Router getRouter() {
		return router;
	}

	/**
	 * 
	 * @return the conf for this node.
	 */
	public synchronized Config getConf() {
		return conf;
	}

	/**
	 * 
	 *  @return the ops processor for this node.
	 */
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
	 * Load a topology's classes into a class loader.
	 * 
	 * @param topologyId the name of the topology to load
	 * @return true if loaded successfully, false otherwise
	 */
	public synchronized boolean loadJarFile(String topologyId) {
		Path pathname = Paths.get(conf.getJarPath() + "/" + comms.getMyNodeDesc(), topologyId);
		ClassLoader mainLoader = Node.class.getClassLoader(); // some class in the main application
		JarInputStream crunchifyJarFile = null;
		try {
			log.info("creating class loader for: "+topologyId);
			pluginClasses.put(topologyId,new HashSet<String>());
			pluginLoaders.put(topologyId,URLClassLoader.newInstance(new URL[]{pathname.toUri().toURL()}, mainLoader));
			crunchifyJarFile = new JarInputStream(new FileInputStream(pathname.toString()));
			JarEntry crunchifyJar;
 
			while (true) {
				crunchifyJar = crunchifyJarFile.getNextJarEntry();
				if (crunchifyJar == null) {
					break;
				}
				if ((crunchifyJar.getName().endsWith(".class"))) {
					String className = crunchifyJar.getName().replaceAll("/", "\\.");
					String myClass = className.substring(0, className.lastIndexOf('.'));
					//log.debug("loading className: "+className+" class: "+myClass);
					try {
						pluginLoaders.get(topologyId).loadClass(myClass);
						pluginClasses.get(topologyId).add(myClass);
					} catch (ClassNotFoundException e) {
						log.warn("class not found: "+myClass);
					} catch (java.lang.NoClassDefFoundError e) {
						log.warn("no class def: "+myClass);
					}
					
				}
			}
			return true;
		} catch (MalformedURLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				crunchifyJarFile.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
//		try {
//			Agent.addToClassPath(new File(pathname.toString()));
//			return true;
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
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
	 * @throws DragonInvalidStateException if the topology can not be started
	 */
	public synchronized void startTopology(String topologyId) throws DragonTopologyException, DragonInvalidStateException {
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
	 * @throws DragonInvalidStateException if the topology can not enter the terminating state
	 */
	public synchronized void terminateTopology(String topologyId, GroupOp go) throws DragonTopologyException, DragonInvalidStateException {
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
		ttgo.sendSuccess();
	}

	/**
	 * Remove the name topology from the collection of local clusters. The topology
	 * must be completely terminated, i.e. over all daemons, before removing it. A
	 * garbage collection is called after removal. Router queues for the topology
	 * are also removed. If the topology is misbehaving, then it may be necessary to call
	 * this function even though the topology has not terminated, in which case
	 * try setting purge to true, which will at least interrupt all of the threads.
	 * 
	 * @param topologyId the name of the topology to remove
	 * @param purge is true if remove should try to forcibly remove the topology
	 * @throws DragonTopologyException if the topology does not exist
	 */
	public synchronized void removeTopo(String topologyId,boolean purge) throws DragonTopologyException {
		if (!localClusters.containsKey(topologyId)) {
			if(!purge) throw new DragonTopologyException("topology does not exist [" + topologyId+"]");
			return; // when purging, we don't indicate error on non-existent topology
		}
		if(purge) {
			log.warn("purging topology ["+topologyId+"]");
			localClusters.get(topologyId).closeAll();
			localClusters.get(topologyId).interruptAll();
		}
		router.terminateTopology(topologyId, localClusters.get(topologyId).getTopology());
		localClusters.remove(topologyId);
		pluginLoaders.put(topologyId,null);
		pluginClasses.get(topologyId).clear();
		System.gc();
		
	}

	/**
	 * Autonomously called by a local cluster in the case of user code errors, that
	 * signals other daemons to halt the topology.
	 * 
	 * @param topologyId the name of the topology that has failed
	 */
	public synchronized void signalHaltTopology(String topologyId) {
		try {
			operationsThread.newHaltTopoGroupOp(topologyId, (op)->{
				log.warn("halting topology, waiting up to ["+getConf().getDragonServiceTimeoutMs()/1000+"] seconds...");
			},	(op) -> {
				log.warn("topology was halted due to too many errors");
			}, (op, error) -> {
				try {
					topologyFault(topologyId);
				} catch (DragonTopologyException e) {
					e.printStackTrace();
					log.error(e);
				}
			}).onRunning((op) -> {
				try {
					haltTopology(topologyId);
				} catch (DragonTopologyException | DragonInvalidStateException e) {
					op.fail(e.getMessage());
				}
			}).onTimeout(getTimer(), getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op)->{
				op.fail("timed out waiting for nodes to respond");
			});
		} catch (DragonInvalidContext e) {
			try {
				topologyFault(topologyId);
			} catch (DragonTopologyException e2) {
				e2.printStackTrace();
				log.error(e2);
			}
		}
	}

	/**
	 * Halt the local topology by suspending all of its threads.
	 * 
	 * @param topologyId the name of the topology to halt
	 * @throws DragonTopologyException if the topology does not exist
	 * @throws DragonInvalidStateException if the topology can not be halted 
	 */
	public synchronized void haltTopology(String topologyId) throws DragonTopologyException, DragonInvalidStateException {
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
		HashMap<String, List<String>> comps = new HashMap<>();
		HashMap<String,HashMap<String, Sample>> metrics = new HashMap<>();
		HashMap<String, HashMap<String, ArrayList<ComponentError>>> errors = new HashMap<>();
		for (String topologyId : localClusters.keySet()) {
			state.put(topologyId, localClusters.get(topologyId).getState().name());
			comps.put(topologyId, new ArrayList<String>());
			metrics.put(topologyId,new HashMap<String,Sample>());
			for(String spoutid : localClusters.get(topologyId).getSpouts().keySet()) {
				for(Integer taskId : localClusters.get(topologyId).getSpouts().get(spoutid).keySet()) {
					comps.get(topologyId).add(spoutid+":"+taskId);
					metrics.get(topologyId).put(spoutid+":"+taskId, new Sample(localClusters.get(topologyId).getSpouts().get(spoutid).get(taskId)));
				}
			}
			for(String boltid : localClusters.get(topologyId).getBolts().keySet()) {
				for(Integer taskId : localClusters.get(topologyId).getBolts().get(boltid).keySet()) {
					comps.get(topologyId).add(boltid+":"+taskId);
					metrics.get(topologyId).put(boltid+":"+taskId, new Sample(localClusters.get(topologyId).getBolts().get(boltid).get(taskId)));
				}
			}
			errors.put(topologyId, new HashMap<String, ArrayList<ComponentError>>());
			for (Component component : localClusters.get(topologyId).getComponentErrors().keySet()) {
				String name = component.getComponentId() + ":" + component.getTaskId();
				errors.get(topologyId).put(name, localClusters.get(topologyId).getComponentErrors().get(component));
			}
		}
		
		/*
		 * Store the data into the holding variables prior to sending the response.
		 */
		ltgo.metrics=metrics;
		ltgo.components = comps;
		ltgo.state = state;
		ltgo.errors = errors;
	}

	/**
	 * Resume a halted topology, by signalling all the threads that they can
	 * continue.
	 * 
	 * @param topologyId the name of the topology to resume
	 * @throws DragonTopologyException if the topology does not exist
	 * @throws DragonInvalidStateException if the topology can not change to halted state
	 */
	public synchronized void resumeTopology(String topologyId) throws DragonTopologyException, DragonInvalidStateException {
		if (!localClusters.containsKey(topologyId))
			throw new DragonTopologyException("topology does not exist: " + topologyId);
		localClusters.get(topologyId).resumeTopology();
	}
	
	/**
	 * Start up a number numDaemons of new Dragon daemons with a given partitionId.
	 * Service and data ports increase by 10 for each new daemon started.
	 * @param partitionId
	 * @param numDaemons
	 * @throws IOException 
	 */
	public synchronized int allocatePartition(String partitionId,int numDaemons) {
		log.debug("allocating ["+numDaemons+"] partitions ["+partitionId+"]");
		int size = childProcesses.keySet().size();
		for(int i=0;i<numDaemons;i++) {
			Config c = new Config();
			c.putAll(conf);
			c.put(Config.DRAGON_NETWORK_PARTITION,partitionId);
			c.put(Config.DRAGON_NETWORK_PRIMARY,false);
			c.put(Config.DRAGON_NETWORK_LOCAL_SERVICE_PORT,conf.getDragonNetworkLocalServicePort()+(size+1)*2);
			c.put(Config.DRAGON_NETWORK_LOCAL_DATA_PORT,conf.getDragonNetworkLocalDataPort()+(size+1)*2);
			c.put(Config.DRAGON_NETWORK_PARENT,comms.getMyNodeDesc().toMap());
			String home = c.getDragonHomeDir();
			try {
				writeConf(c,home+"/conf/dragon-"+c.getDragonNetworkLocalDataPort()+".yaml");
				NodeDescriptor child=c.getLocalHost();
				ProcessBuilder pb = ProcessManager.createDaemon(c);
				
				processManager.startProcess(pb,false,(p)->{
					Process proc = (Process) p;
					childProcesses.put(child,p);
					try {
						proc.getInputStream().close();
					} catch (IOException e) {
						log.warn("could not close the input stream to the process");
					}
				},(pb2)->{
					log.error("process failed to start: "+pb.toString());
				},(p)->{
					log.warn("process has exited: "+p.exitValue());
				});
			} catch (IOException e) {
				return i;
			}
			size++;
		}
		return numDaemons;
	}
	
	/**
	 * Deallocate partitions
	 * @param partitionId
	 * @param numDaemons
	 * @return
	 */
	public synchronized int deallocatePartition(String partitionId,int numDaemons) {
		int killed=0;
		HashSet<NodeDescriptor> deleted = new HashSet<>();
		for(NodeDescriptor node : childProcesses.keySet()) {
			if(numDaemons==0) break;
			if(node.getPartition().equals(partitionId)) {
				// kill node
				// the following does not work reliably, if at all
				//childProcesses.get(node).destroyForcibly(); 
				// so...
				
				
				deleted.add(node);
				killed++;
				numDaemons--;
			}
		}
		for(NodeDescriptor nodeDescriptor : deleted) {
			childProcesses.remove(nodeDescriptor);
		}
		return killed;
	}
	
	/**
	 * Utility function to write a config to a file.
	 * @param conf
	 * @param filename
	 * @throws IOException 
	 */
	public static void writeConf(Config conf, String filename) throws IOException {
		File file = new File(filename);
		FileOutputStream fos = new FileOutputStream(file);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		bw.write(conf.toYamlStringNice());
		bw.newLine();
		bw.close();
	}
	
	/**
	 * Return a status object describing the current state of the node.
	 */
	public synchronized NodeStatus getStatus() {
		NodeStatus nodeStatus = new NodeStatus();
		nodeStatus.desc=comms.getMyNodeDesc();
		nodeStatus.timestamp=Instant.now().toEpochMilli();
		nodeStatus.state=nodeState;
		for(String topologyId : localClusters.keySet()) {
			nodeStatus.localClusterStates.put(topologyId,localClusters.get(topologyId).getState());
		}
		nodeStatus.partitionId=conf.getDragonNetworkPartition();
		nodeStatus.primary=conf.getDragonNetworkPrimary();
		nodeStatus.parent=parentDesc;
		return nodeStatus;
	}
	
	/**
	 * terminate this node and quit the jvm
	 */
	public synchronized void terminate() {
		log.info("going down...");
		comms.close();
		setNodeState(NodeState.TERMINATING);
		if(!localClusters.isEmpty()) {
			log.error("topologies are still allocated");
			localClusters.forEach((topologyId,localCluster)->{
				localCluster.interruptAll();
			});
		}
		metricsThread.interrupt();
		timer.cancel();
		router.terminate();
		processManager.interrupt();
		operationsThread.interrupt();
		serviceThread.interrupt();
		nodeThread.interrupt();
	}
	
	/**
	 * @return timer
	 */
	public synchronized Timer getTimer() {
		return timer;
	}
	
	/**
	 * Called when a node is locally detected to no longer be 
	 * reachable due to comms timeout.
	 */
	public synchronized void nodeFault(NodeDescriptor desc) {
		log.error("["+desc+"] is not reachable");
		removeNode(desc);
		HashSet<NodeDescriptor> more = new HashSet<>();
		for(NodeDescriptor desc2 : nodeThread.getAliveContext().values()) {
			if(!desc2.equals(comms.getMyNodeDesc())) {
				try {
					comms.sendNodeMsg(desc2, new NodeFaultNMsg(desc));
				} catch (DragonCommsException e) {
					more.add(desc2);
				}
			}
		}
		// signal any further faults
		for(NodeDescriptor desc2 : more) {
			nodeFault(desc2);
		}
	}
	
	/**
	 * Called when receiving a message that a node is dead. Also called
	 * as part of the local node fault detection method {@link #nodeFault(NodeDescriptor)}.
	 * @param desc
	 */
	public synchronized void removeNode(NodeDescriptor desc) {
		if(nodeThread.getAliveContext().containsKey(desc.toString())) {
			log.debug("removing ["+desc+"] from the node context");
			nodeThread.setDead(desc);
			// tell the node its dead, just in case it doesn't know
			Ops.inst().newOp((op)->{
				try {
					Node.inst().getComms().sendNodeMsg(desc, new RipNMsg());
				} catch (DragonCommsException e) {
					op.fail("["+desc+"] is really dead");
				}
			}, (op)->{
				op.success();
			}, (op)->{
				log.info("sent RIP to ["+desc+"]");
			}, (op,msg)->{
				log.warn(msg);
			});
			// trigger topology faults as required
			for(String topologyId : localClusters.keySet()) {
				if(localClusters.get(topologyId).getTopology().getReverseEmbedding().containsKey(desc)){
					try {
						topologyFault(topologyId);
					} catch (DragonTopologyException e) {
						e.printStackTrace();
						log.error(e.getMessage());
					}
				}
			}
		} // else may have already been removed due to a node fault message
	}
	
	/**
	 * Called when a topology is locally detected to need to go into
	 * the fault state.
	 * @throws DragonTopologyException 
	 */
	public synchronized void topologyFault(String topologyId) throws DragonTopologyException {
		if(!localClusters.containsKey(topologyId)) {
			throw new DragonTopologyException("topology does not exist ["+topologyId+"]");
		}
		if(localClusters.get(topologyId).getState()==LocalCluster.State.FAULT) {
			// topology has already been faulted
			return;
		}
		log.error("topology ["+topologyId+"] has faulted");
		for (NodeDescriptor desc : localClusters.get(topologyId).getTopology().getReverseEmbedding().keySet()) {
			if(nodeThread.getAliveContext().containsKey(desc.toString()) && !desc.equals(comms.getMyNodeDesc())) {
				Ops.inst().newOp((op)->{
					try {
						comms.sendNodeMsg(desc, new TopoFaultNMsg(topologyId));
					} catch (DragonCommsException e) {
						nodeFault(desc);
						op.fail("topology ["+topologyId+"] may not be faulted correctly");
					}
				}, (op)->{
					op.success();
				}, (op)->{
					log.info("sent topology fault to ["+desc+"]");
				}, (op,msg)->{
					log.warn(msg);
				});
			} else {
				localClusters.get(topologyId).setFault();
			}
		}
	}
	
	/**
	 * Called when receiving a message that a topology has faulted.
	 * @param topologyId
	 * @throws DragonTopologyException
	 */
	public synchronized void setTopologyFault(String topologyId) throws DragonTopologyException {
		if(!localClusters.containsKey(topologyId)) {
			throw new DragonTopologyException("topology does not exist ["+topologyId+"]");
		}
		// this method will just return if the topology is already at fault
		localClusters.get(topologyId).setFault();
	}

	/**
	 * Called when the topology is locally detected to have faulted 
	 * and has not yet been instantiated.
	 * @param topologyId
	 * @param dragonTopology
	 */
	public synchronized void topologyFault(String topologyId,DragonTopology dragonTopology) {
		log.error("topology ["+topologyId+"] has faulted");
		for (NodeDescriptor desc : dragonTopology.getReverseEmbedding().keySet()) {
			if(nodeThread.getAliveContext().containsKey(desc.toString()) && !desc.equals(comms.getMyNodeDesc())) {
				Ops.inst().newOp((op)->{
					try {
						comms.sendNodeMsg(desc, new TopoFaultNMsg(topologyId));
					} catch (DragonCommsException e) {
						nodeFault(desc);
						op.fail("topology ["+topologyId+"] may not be faulted correctly");
					}
				}, (op)->{
					op.success();
				}, (op)->{
					log.info("sent topology fault to ["+desc+"]");
				}, (op,msg)->{
					log.warn(msg);
				});
			} else {
				localClusters.get(topologyId).setFault();
			}
		}
		
	}

	/**
	 * Called when the node has been considered dead, it must now wait to
	 * receive a context update.
	 */
	public void rip() {
		log.warn("received RIP :-(");
		ArrayList<String> topos = new ArrayList<String>(localClusters.keySet());
		topos.forEach((topologyId)->{
			try {
				removeTopo(topologyId,true);
			} catch (DragonTopologyException e) {
				log.error("topology was removed elsewhere");
			}
		});
		nodeThread.setAllDead();
	}
}
