package dragon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.grouping.AbstractGrouping;
import dragon.network.DragonTopologyException;
import dragon.network.Node;
import dragon.network.operations.GroupOp;
import dragon.network.operations.TermTopoGroupOp;
import dragon.spout.SpoutOutputCollector;
import dragon.task.InputCollector;
import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.BoltDeclarer;
import dragon.topology.DragonTopology;
import dragon.topology.GroupingsSet;
import dragon.topology.OutputFieldsDeclarer;
import dragon.topology.SpoutDeclarer;
import dragon.topology.StreamMap;
import dragon.topology.base.Bolt;
import dragon.topology.base.Component;
import dragon.topology.base.Spout;
import dragon.tuple.Fields;
import dragon.tuple.NetworkTask;
import dragon.tuple.Tuple;
import dragon.tuple.Values;
import dragon.utils.CircularBlockingQueue;
import dragon.utils.NetworkTaskBuffer;

/**
 * LocalCluster manages the bolts and spouts for a given topology that are
 * within this daemon.
 * 
 * The life cycle of a topology goes through several states:
 * <ul>
 * <li>ALLOCATED The topology exists in memory but is not ready to be run.</li>
 * <li>SUBMITTED The topology is ready to be run, open/prepare methods 
 * have not yet been called.</li>
 * <li>RUNNING The topology is currently running.</li>
 * <li>HALTED All threads for the topology have been suspended.</li>
 * <li>TERMINATING No new data is being emitted from spouts. When all bolts have no 
 * more work to do, the topology is removed from memory.</li>
 * <li>FAULT The topology is at fault. All threads are interrupted.</li>
 * </ul>
 * 
 * The states may transition in the following ways:
 * <ul>
 * <li>ALLOCATED => SUBMITTED An allocated topology can only move to the submitted or fault state.</li>
 * <li>SUBMITTED => RUNNING A submitted topology can only move to the running or fault state. 
 * The open/prepare methods are called during this transition.</li>
 * <li>RUNNING => TERMINATING A running topology may be terminated by the user.</li>
 * <li>RUNNING => HALTED A running topology may be terminated by the user. 
 * The topology may also be halted automatically if it throws too many exceptions.</li> 
 * <li>HALTED => RUNNING The user may resume the topology.</li>
 * <li>HALTED => TERMINATING The user may terminate a halted topology.</li>
 * <li>ALLOCATED,SUBMITTED,RUNNING,TERMINATING,HALTED => FAULT A fault has occurred that requires the 
 * topology to be purged and restarted. All threads have been interrupted.</li>
 * </ul>
 * 
 * @author aaron
 *
 */
public class LocalCluster {
	private final static Logger log = LogManager.getLogger(LocalCluster.class);
	
	/**
	 * A reference to the node for this daemon.
	 */
	private final Node node;
	
	/**
	 * Map from component id to task index to instance of bolt, for only those
	 * instances in the topology that are allocated to this daemon.
	 */
	private HashMap<String,HashMap<Integer,Bolt>> bolts;
	
	/**
	 * Map from component id to task index to instance of spout, for only those
	 * instances in the topology that are allocated to this daemon.
	 */
	private HashMap<String,HashMap<Integer,Spout>> spouts;
	
	/**
	 * Queue of queues of NetworkTasks, i.e. the outputs of spouts and bolts,
	 * that are waiting to be serviced. One reference to such a queue is placed
	 * on outputsPending for each NetworkTask it contains.
	 */
	private CircularBlockingQueue<NetworkTaskBuffer>[] outputsPending;

	/**
	 * Map from component id to the conf for spouts, for only those instances
	 * in the topology that are allocated to this daemon.
	 */
	private HashMap<String,Config> spoutConfs;
	
	/**
	 * Map from component id to the conf for bolts, for only those instances
	 * in the topology that are allocated to this daemon.
	 */
	private HashMap<String,Config> boltConfs;
	
	/**
	 * Threads that are allocated to execute the run() methods of bolts and spouts.
	 * There may be less threads than total number of instances of bolts and spouts
	 * combined, allocated on this daemon. However the number of threads must be at
	 * least one more than the number of spout instances allocated on this daemon,
	 * because spout threads are allowed to block in the run() method.
	 * There does not need to be more threads than the total number of bolts and
	 * spouts combined, allocated on this daemon.
	 */
	private ArrayList<Thread> componentExecutorThreads;
	
	/**
	 * Threads that are allocated to transfer NetworkTasks from the outputs of 
	 * components to their destinations in this local cluster, or from the Router
	 * (that are incoming from a remote daemon) to their destinations in this
	 * local cluster. These threads never block when a destination buffer is full,
	 * but rather continue to poll. Therefore the number of these threads can be as
	 * little as one.
	 */
	private ArrayList<Thread> networkExecutorThreads;
	
	/**
	 * Map from component reference to array list of component errors that the
	 * component has thrown on this local cluster.
	 */
	private HashMap<Component,ArrayList<ComponentError>> componentErrors;
	
	/**
	 * The possible states of the local cluster.
	 * <li>{@link #ALLOCATED}</li>
	 * <li>{@link #SUBMITTED}</li>
	 * <li>{@link #PREPROCESSING}</li>
	 * <li>{@link #RUNNING}</li>
	 * <li>{@link #TERMINATING}</li>
	 * <li>{@link #HALTED}</li>
	 * <li>{@link #FAULT}</li>
	 */
	public static enum State {
		/**
		 * The state that is set when the local cluster is constructed.
		 * A transient state (only held momentarily).
		 */
		ALLOCATED,
		
		/**
		 * The local cluster is ready to use. A transient state.
		 */
		SUBMITTED,
		
		/**
		 * The local cluster is busy doing pre-processing tasks, prior to running.
		 * A transient state, but may take some time depending on the complexity of
		 * the topology.
		 */
		PREPROCESSING,
		
		/**
		 * The local cluster is now processing tuples.
		 */
		RUNNING,
		
		/**
		 * The local cluster is terminating, no more tuples are being emitted at spouts.
		 */
		TERMINATING,
		
		/**
		 * The local cluster's threads have been suspended and it can be resumed.
		 */
		HALTED,
		
		/**
		 * A fault has occurred and the topology will need to be restart.
		 */
		FAULT
	}
	
	/**
	 * The state of the local cluster.
	 */
	private volatile State state;
	
	/**
	 * Lock for halting the threads in the local cluster.
	 */
	private final ReentrantLock haltLock = new ReentrantLock();
	
	/**
	 * Condition for threads in the local cluster to wait on.
	 */
	private final Condition restartCondition = haltLock.newCondition();
	
	/**
	 * Set of outstanding group operations for this local cluster to respond to.
	 * E.g. when the cluster terminates, there will be a group operation that
	 * it should respond to, to indicate that termination is successful.
	 * The map is from the class of the group operation, to a set of such group
	 * operations for that class that are waiting for an outcome.
	 */
	@SuppressWarnings("rawtypes")
	private HashMap<Class,HashSet<GroupOp>> groupOperations;
	
	/**
	 * Name of the topology on this local cluster.
	 */
	private String topologyName;
	
	/**
	 * Conf applied to the topology on this local cluster, which is the conf
	 * for the node, overridden by variables in the conf supplied when submitting
	 * the topology. This allows the programmer to override some aspects of this
	 * local cluster on a per topology basis, like queue lengths, etc.
	 */
	private Config conf;
	
	/**
	 * The complete topology, a part or all of which is allocated on this
	 * local cluster.
	 */
	private DragonTopology dragonTopology;

	/**
	 * A thread that sleeps for 1 second at a time, and causes the tickCounterThread
	 * to increment counters etc., every second.
	 */
	private Thread tickThread;
	
	/**
	 * A thread that generates tick tuples for bolts as required by their conf.
	 */
	private Thread tickCounterThread;
	
	/**
	 * The number of ticks of the tick thread.
	 */
	private long tickTime=0;
	
	/**
	 * The number of ticks that the ticker counter thread has processed.
	 */
	private long tickCounterTime=0;
	
	/**
	 * The number of ticks that each bolt component has reached.
	 */
	private HashMap<String,Integer> boltTickCount;
	
	/**
	 * The fields of a tick tuple.
	 */
	private final Fields tickFields=new Fields("tick");
	
	/**
	 * The amount of parallelism for the component threads that has been computed
	 * based on the number of spout instances and parallelism hints provided on the
	 * bolt instances.
	 */
	private int totalParallelismHint=0;
	
	
	/*
	 * Spouts and bolts of a prepared topology wont be opened/prepared until a start
	 * message is received. This ensures that the topology is successfully prepared on
	 * all Dragon daemons before triggering any topology application code. The following
	 * classes and members maintain a list of spouts and bolts that will be opened/prepared
	 * when the start message is received.
	 */
	
	/**
	 * Container class for bolt instances on this local cluster that need to be prepared,
	 * when the start message is received.
	 */
	private class BoltPrepare {
		public Bolt bolt;
		public TopologyContext context;
		public OutputCollector collector;
		public BoltPrepare(Bolt bolt,TopologyContext context,OutputCollector collector) {
			this.bolt=bolt;
			this.context=context;
			this.collector=collector;
		}
	}
	
	/**
	 * Array list of bolt instances that need to be prepared, when the start message is received. 
	 */
	private ArrayList<BoltPrepare> boltPrepareList;
	
	/**
	 * Container class for spout instances on this local cluster that need to be prepared,
	 * when the start message is received.
	 */
	private class SpoutOpen {
		public Spout spout;
		public TopologyContext context;
		public SpoutOutputCollector collector;
		public SpoutOpen(Spout spout,TopologyContext context,SpoutOutputCollector collector) {
			this.spout=spout;
			this.context=context;
			this.collector=collector;
		}
	}
	
	/**
	 * Array list of spout instances that need to be prepared, when the start message is received.
	 */
	private ArrayList<SpoutOpen> spoutOpenList;
	
	/*
	 * Initializers
	 */
	
	
	/**
	 * Initializer for running in local mode.
	 */
	@SuppressWarnings("rawtypes")
	public LocalCluster(){
		this.node=null;
		state=State.ALLOCATED;
		groupOperations = new HashMap<Class,HashSet<GroupOp>>();
		componentErrors = new  HashMap<Component,ArrayList<ComponentError>>();
	}
	
	/**
	 * Initializer for running in network mode.
	 * @param node the node that is managing this local cluster instance
	 */
	@SuppressWarnings("rawtypes")
	public LocalCluster(Node node) {
		this.node=node;
		state=State.ALLOCATED;
		groupOperations = new HashMap<Class,HashSet<GroupOp>>();
		componentErrors = new  HashMap<Component,ArrayList<ComponentError>>();
	}
	
	/**
	 * Submit a topology and run it immediately, useful only in local mode. This
	 * method should only be called in local mode since it exits the system if
	 * exceptions are thrown.
	 * @param topologyName the name of the topology
	 * @param conf the conf for the topology
	 * @param dragonTopology the topology
	 */
	public void submitTopology(String topologyName, Config conf, DragonTopology dragonTopology) {
		Config lconf=null;
		
		try {
			lconf = new Config(Constants.DRAGON_PROPERTIES,true);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		lconf.putAll(conf);
		try {
			submitTopology(topologyName, lconf, dragonTopology, true);
		} catch (DragonRequiresClonableException | DragonTopologyException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * Submit a toplogy and run it if start is true, otherwise wait for a start topology message.
	 * This method determines which spout and bolt instances will run on this local cluster, based
	 * on the topology embedding information. In local mode, the embedding information is null, and
	 * all instances will run on this local cluster, otherwise only selected instances will run.
	 * @param topologyName the name of the topology
	 * @param conf the conf for the topology
	 * @param dragonTopology the topology
	 * @param start whether to start the topology immediately or not
	 * @throws DragonRequiresClonableException if the topology contains components that are not clonable
	 * @throws DragonTopologyException 
	 */
	@SuppressWarnings("unchecked")
	public void submitTopology(String topologyName, Config conf, DragonTopology dragonTopology, boolean start) throws DragonRequiresClonableException, DragonTopologyException {
		this.topologyName=topologyName;
		this.conf=conf;
		this.dragonTopology=dragonTopology;
		
		
		/*
		 * Prepare the groupings. This must be done before preparing/opening bolts/spouts, because
		 * the outputs field declaration will tell the grouping which fields to expect, that may be
		 * of use for the grouping, in particular for the fields grouping.
		 */
		for(String fromComponentId : dragonTopology.getSpoutMap().keySet()) {
			log.debug("preparing groupings for spout["+fromComponentId+"]");
			HashMap<String,StreamMap> 
				fromComponent = dragonTopology.getDestComponentMap(fromComponentId);
			if(fromComponent==null) {
				throw new DragonTopologyException("spout ["+fromComponentId+"] has no components listening to it");
			}
			for(String toComponentId : fromComponent.keySet()) {
				HashMap<String,GroupingsSet> 
					streams = dragonTopology.getStreamMap(fromComponentId, toComponentId);
				BoltDeclarer boltDeclarer = dragonTopology.getBoltMap().get(toComponentId);
				ArrayList<Integer> taskIndices=new ArrayList<Integer>();
				for(int i=0;i<boltDeclarer.getNumTasks();i++) {
					taskIndices.add(i);
				}
				for(String streamId : streams.keySet()) {
					GroupingsSet groupings = dragonTopology.getGroupingsSet(fromComponentId, toComponentId, streamId);
					for(AbstractGrouping grouping : groupings) {
						grouping.prepare(null, null, taskIndices);
					}
				}
			}
			
		}
		
		for(String fromComponentId : dragonTopology.getBoltMap().keySet()) {
			log.debug("preparing groupings for bolt["+fromComponentId+"]");
			HashMap<String,StreamMap> 
				fromComponent = dragonTopology.getDestComponentMap(fromComponentId);
			if(fromComponent==null) {
				log.debug(fromComponentId+" is a sink");
				continue;
			}
			log.debug(fromComponent);
			for(String toComponentId : fromComponent.keySet()) {
				HashMap<String,GroupingsSet> 
					streams = dragonTopology.getStreamMap(fromComponentId, toComponentId);
				BoltDeclarer boltDeclarer = dragonTopology.getBoltMap().get(toComponentId);
				ArrayList<Integer> taskIndices=new ArrayList<Integer>();
				for(int i=0;i<boltDeclarer.getNumTasks();i++) {
					taskIndices.add(i);
				}
				for(String streamId : streams.keySet()) {
					GroupingsSet groupings = dragonTopology.getGroupingsSet(fromComponentId, toComponentId, streamId);
					for(AbstractGrouping grouping : groupings) {
						grouping.prepare(null, null, taskIndices);
					}
				}
			}
			
		}
		
		/*
		 * Allocate an array of threads for the outputs pending queue. These threads wont
		 * be created until the system is started.
		 */
		networkExecutorThreads = new ArrayList<Thread>();
		
		/*
		 * If we are not starting the topology immediately then allocated holding arrays
		 * to go over later when open spouts and preparing bolts.
		 */
		if(!start) {
			spoutOpenList = new ArrayList<SpoutOpen>();
			boltPrepareList = new ArrayList<BoltPrepare>();
		}
		
		int totalOutputsBufferSize=0;
		int totalInputsBufferSize=0;
		int spoutsAllocated=0;
		
		// allocate spouts and open them
		spouts = new HashMap<String,HashMap<Integer,Spout>>();
		spoutConfs = new HashMap<String,Config>();
		for(String spoutId : dragonTopology.getSpoutMap().keySet()) {
			boolean localcomponent = node==null || !(dragonTopology.getReverseEmbedding()!=null &&
					!dragonTopology.getReverseEmbedding().contains(node.getComms().getMyNodeDesc(),spoutId));
			HashMap<Integer,Spout> hm=null;
			SpoutDeclarer spoutDeclarer = dragonTopology.getSpoutMap().get(spoutId);
			ArrayList<Integer> taskIndices=new ArrayList<Integer>();
			for(int i=0;i<spoutDeclarer.getNumTasks();i++) {
				taskIndices.add(i);
			}
			if(localcomponent) {
				log.debug("allocating spout ["+spoutId+"]");
				spouts.put(spoutId, new HashMap<Integer,Spout>());
				hm = spouts.get(spoutId);
				Map<String,Object> bc = spoutDeclarer.getSpout().getComponentConfiguration();
				Config spoutConf = new Config();
				spoutConf.putAll(bc);
				spoutConfs.put(spoutId, spoutConf);
			}
			int numAllocated=0;
			for(int i=0;i<spoutDeclarer.getNumTasks();i++) {
				boolean localtask = node==null || !(dragonTopology.getReverseEmbedding()!=null &&
						!dragonTopology.getReverseEmbedding().contains(node.getComms().getMyNodeDesc(),spoutId,i));
				try {
					if(localtask) {
						numAllocated++;
						spoutsAllocated++;
					}
					Spout spout=(Spout) spoutDeclarer.getSpout().clone();
					if(localtask) hm.put(i, spout);
					OutputFieldsDeclarer declarer = new OutputFieldsDeclarer(this,spoutId);
					spout.declareOutputFields(declarer);
					spout.setOutputFieldsDeclarer(declarer);
					TopologyContext context = new TopologyContext(spoutId,i,taskIndices);
					spout.setTopologyContext(context);
					spout.setLocalCluster(this);
					SpoutOutputCollector collector = new SpoutOutputCollector(this,spout);
					if(localtask) totalOutputsBufferSize+=collector.getTotalBufferSpace();
					spout.setOutputCollector(collector);
					if(localtask) {
						if(start) {
							try {
								spout.open(conf, context, collector);
							} catch (Throwable e) {
								e.printStackTrace();
								throw new DragonTopologyException("spout ["+spout.getInstanceId()+"] throw exception when opening: "+e.getMessage());
							}
						} else {
							openLater(spout,context,collector);
						}
					}
				} catch (CloneNotSupportedException e) {
					throw new DragonRequiresClonableException("bolts and spouts must be cloneable: "+spoutDeclarer.getSpout().getComponentId());
				}
			}
			// for spouts we should have one thread per instance since nextTuple() can block if the spout is waiting for more data
			totalParallelismHint+=numAllocated;
		}
		
		// allocate bolts and prepare them
		bolts = new HashMap<String,HashMap<Integer,Bolt>>();
		boltConfs = new HashMap<String,Config>();
		for(String boltId : dragonTopology.getBoltMap().keySet()) {
			boolean localcomponent = !(dragonTopology.getReverseEmbedding()!=null &&
					!dragonTopology.getReverseEmbedding().contains(node.getComms().getMyNodeDesc(),boltId));
			HashMap<Integer,Bolt> hm=null;
			BoltDeclarer boltDeclarer = dragonTopology.getBoltMap().get(boltId);
			ArrayList<Integer> taskIndices=new ArrayList<Integer>();
			for(int i=0;i<boltDeclarer.getNumTasks();i++) {
				taskIndices.add(i);
			}
			if(localcomponent) {
				log.debug("allocating bolt ["+boltId+"]");
				bolts.put(boltId, new HashMap<Integer,Bolt>());
				hm = bolts.get(boltId);
				Map<String,Object> bc = boltDeclarer.getBolt().getComponentConfiguration();
				Config boltConf = new Config();
				boltConf.putAll(bc);
				boltConfs.put(boltId, boltConf);
			}
			int numAllocated=0;
			for(int i=0;i<boltDeclarer.getNumTasks();i++) {
				boolean localtask = !(dragonTopology.getReverseEmbedding()!=null &&
						!dragonTopology.getReverseEmbedding().contains(node.getComms().getMyNodeDesc(),boltId,i));
				try {
					if(localtask) numAllocated++;
					Bolt bolt=(Bolt) boltDeclarer.getBolt().clone();
					if(localtask) hm.put(i, bolt);
					OutputFieldsDeclarer declarer = new OutputFieldsDeclarer(this,boltId);
					bolt.declareOutputFields(declarer);
					bolt.setOutputFieldsDeclarer(declarer);
					TopologyContext context = new TopologyContext(boltId,i,taskIndices);
					bolt.setTopologyContext(context);
					InputCollector inputCollector = new InputCollector(this, bolt);
					if(localtask) totalInputsBufferSize+=getConf().getDragonInputBufferSize();
					bolt.setInputCollector(inputCollector);
					bolt.setLocalCluster(this);
					OutputCollector collector = new OutputCollector(this,bolt);
					if(localtask) totalOutputsBufferSize+=collector.getTotalBufferSpace();
					bolt.setOutputCollector(collector);
					if(localtask) {
						if(start) {
							try {
								bolt.prepare(conf, context, collector);
							} catch (Throwable e) {
								e.printStackTrace();
								throw new DragonTopologyException("bolt ["+bolt.getInstanceId()+"] threw exception when preparing: "+e.getMessage());
							}
						} else {
							prepareLater(bolt,context,collector);
						}
					}
				} catch (CloneNotSupportedException e) {
					throw new DragonRequiresClonableException("bolts and spouts must be cloneable: "+boltDeclarer.getBolt().getComponentId());
				}
			}
			totalParallelismHint+=numAllocated;//Math.ceil((double)numAllocated*boltDeclarer.getParallelismHint()/boltDeclarer.getNumTasks());
		}
		
		log.info("total outputs buffer size is "+totalOutputsBufferSize);
		outputsPending = new CircularBlockingQueue[conf.getDragonLocalclusterThreads()];
		for(int i=0;i<conf.getDragonLocalclusterThreads();i++) {
			outputsPending[i]=new CircularBlockingQueue<NetworkTaskBuffer>(2*totalOutputsBufferSize);
		}
		log.info("total inputs buffer size is "+totalInputsBufferSize);
		
		componentExecutorThreads = new ArrayList<Thread>();
		for(HashMap<Integer,Spout> spouts : spouts.values()) {
			for(Component component : spouts.values()) {
				Thread thread=new Thread(){
					@Override
					public void run(){
						log.info("starting up");
						Component thisComponent=component;
						while(!isInterrupted()&&!component.isClosed()){
							if(state==LocalCluster.State.HALTED) {
								log.info("halted");
								try {
									haltLock.lock();
									try {
										restartCondition.await();
									} catch (InterruptedException e) {
										log.info("interrupted");
										break;
									}
									log.info("resuming");
								} finally {
									haltLock.unlock();
								}
								continue;
							}
	
							thisComponent.run();
							
						}
						log.info("shutting down");
					}
				};
				thread.setName(topologyName+":"+component.getInstanceId());
				componentExecutorThreads.add(thread);
			}
		}
		for(HashMap<Integer,Bolt> bolts : bolts.values()) {
			for(Component component : bolts.values()) {
				Thread thread=new Thread(){
					@Override
					public void run(){
						log.info("starting up");
						Component thisComponent=component;
						while(!isInterrupted()){
							if(state==LocalCluster.State.HALTED) {
								log.info("halted");
								try {
									haltLock.lock();
									try {
										restartCondition.await();
									} catch (InterruptedException e) {
										log.info("interrupted");
										break;
									}
									log.info("resuming");
								} finally {
									haltLock.unlock();
								}
								continue;
							}
							thisComponent.run();
						}
						log.info("shutting down");
					}
				};
				thread.setName(topologyName+":"+component.getInstanceId());
				componentExecutorThreads.add(thread);
			}
		}
		
		
		
		
		
		boltTickCount = new HashMap<String,Integer>();
		for(String boltId : bolts.keySet()) {
			if(boltConfs.get(boltId).containsKey(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS)) {
				boltTickCount.put(boltId, (Integer)boltConfs.get(boltId).get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS));
			}
		}
		
		tickCounterThread = new Thread() {
			public void run() {
				this.setName("tick counter");
				while(state!=LocalCluster.State.TERMINATING && !isInterrupted()) {
					if(state==LocalCluster.State.HALTED) {
						log.info(getName()+" halted");
						try {
							haltLock.lock();
							try {
								restartCondition.await();
							} catch (InterruptedException e) {
								log.info(getName()+" interrupted");
								break;
							}
							log.info(getName()+" resuming");
						} finally {
							haltLock.unlock();
						}
					}
					if(tickCounterTime==tickTime) {
						synchronized(tickCounterThread){
							try {
								wait();
							} catch (InterruptedException e) {
								log.info("tick counter thread exiting");
								break;
							}
						}
					}
					while(tickCounterTime<tickTime) {
						for(String boltId:boltTickCount.keySet()) {
							Integer count = boltTickCount.get(boltId);
							count--;
							if(count==0) {
								issueTickTuple(boltId);
								count=(Integer)boltConfs.get(boltId).get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
							}
							boltTickCount.put(boltId, count);
						}
						tickCounterTime++;
					}
				}
			}
		};
		tickCounterThread.setName("tick counter");
		tickCounterThread.start();
		
		tickThread = new Thread() {
			public void run() {
				this.setName("tick");
				while(state!=LocalCluster.State.TERMINATING && !isInterrupted()) {
					if(state==LocalCluster.State.HALTED) {
						log.info(getName()+" halted");
						try {
							haltLock.lock();
							try {
								restartCondition.await();
							} catch (InterruptedException e) {
								log.info(getName()+" interrupted");
								break;
							}
							log.info(getName()+" resuming");
						} finally {
							haltLock.unlock();
						}
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						log.info("terminating tick thread");
						break;
					}
					tickTime++;
					synchronized(tickCounterThread) {
						tickCounterThread.notify();
					}
				}
			}
		};
		tickThread.setName("tick");
		tickThread.start();


		//totalParallelismHint=1;
		
		outputsScheduler();
		
		
		state=State.SUBMITTED;
		if(start) {
			runComponentThreads();
			state=State.RUNNING;
		}
	}
	
	/**
	 * Bolts set to have their prepare function called, at a later time.
	 * @param bolt
	 * @param context
	 * @param collector
	 */
	private void prepareLater(Bolt bolt, TopologyContext context, OutputCollector collector) {
		boltPrepareList.add(new BoltPrepare(bolt,context,collector));
	}
	
	/**
	 * Spouts set to have their open function called, at a later time.
	 * @param spout
	 * @param context
	 * @param collector
	 */
	private void openLater(Spout spout, TopologyContext context, SpoutOutputCollector collector) {
		spoutOpenList.add(new SpoutOpen(spout,context,collector));
	}
	
	/**
	 * Start a topology by opening all its components and scheduling the spouts.
	 * @throws DragonTopologyException 
	 * @throws DragonInvalidStateException 
	 */
	public void openAll() throws DragonTopologyException, DragonInvalidStateException {
		if(state!=State.SUBMITTED) throw new DragonInvalidStateException("state must be "+State.SUBMITTED.name());
		log.info("opening bolts");
		for(BoltPrepare boltPrepare : boltPrepareList) {
			try {
				boltPrepare.bolt.prepare(conf, boltPrepare.context, boltPrepare.collector);
			} catch (Throwable e) {
				e.printStackTrace();
				throw new DragonTopologyException("bolt ["+boltPrepare.bolt.getInstanceId()+"] threw: "+e.toString());
			}
		}
		for(SpoutOpen spoutOpen : spoutOpenList) {
			try {
				spoutOpen.spout.open(conf, spoutOpen.context, spoutOpen.collector);
			} catch (Throwable e) {
				e.printStackTrace();
				throw new DragonTopologyException("spout ["+spoutOpen.spout.getInstanceId()+"] threw: "+e.toString());
			}
		}
		runComponentThreads();
		state=State.RUNNING;
	}

	/**
	 * Send a tick tuple to the given bolt.
	 * @param boltId
	 */
	private void issueTickTuple(String boltId) {
		Tuple tuple=new Tuple();
		tuple.setValues(new Values("0"));
		tuple.setSourceComponent(Constants.SYSTEM_COMPONENT_ID);
		tuple.setSourceStreamId(Constants.SYSTEM_TICK_STREAM_ID);
		for(Bolt bolt : bolts.get(boltId).values()) {
			synchronized(bolt) {
				bolt.setTickTuple(tuple);
			}
		}
	}
	
	/**
	 * Continue to check when all components are closed then terminate all threads
	 * and signal the local cluster has shutdown.
	 */
	private void checkCloseCondition() {
		log.debug("starting shutdown thread");
		Thread shutdownThread = new Thread() {
			@Override
			public void run() {
				// wait for spouts to close
				log.debug("waiting for spouts to close");
				while(true) {
					boolean spoutsClosed=true;
					for(String componentId : spouts.keySet()) {
						HashMap<Integer,Spout> component = spouts.get(componentId);
						for(Integer taskIndex : component.keySet()) {
							Spout spout = component.get(taskIndex);
							if(!spout.isClosed()) {
								spoutsClosed=false;
								break;
							} else {
								log.debug("not closed yet: "+spout.getInstanceId());
							}
						}
						if(spoutsClosed==false) {
							break;
						}
					}
					if(spoutsClosed) break;
					log.info("waiting for spouts to close...");
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						log.warn("interrupted while waiting for spouts to close");
						break;
					}
					
				}
				
				// emit the terminate tuples
				log.debug("emitting terminate tuples");
				for(String componentId : spouts.keySet()) {
					HashMap<Integer,Spout> component = spouts.get(componentId);
					for(Integer taskIndex : component.keySet()) {
						Spout spout = component.get(taskIndex);
						log.debug("spout ["+spout.getInstanceId()+"] emitting terminate tuple");
						spout.getOutputCollector().emitTerminateTuple();
						spout.getOutputCollector().expireAllTupleBundles();
					}
				}
				
				// wait for bolts to close (they only close when they receive the
				// terminate tuple on all of their input streams, from all tasks on
				// those streams).
				log.debug("waiting for bolts to close");
				while(true) {
					boolean alldone=true;
					for(HashMap<Integer,Bolt> boltInstances : bolts.values()) {
						for(Bolt bolt : boltInstances.values()) {
							if(!bolt.isClosed()) {
								alldone=false;
								break;
							}
						}
						if(alldone==false) {
							break;
						}
					}
					if(alldone) break;
					log.info("waiting for work to complete...");
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						log.warn("interrupted while waiting for work to complete");
						break;
					}
				}
				
				// interrupt all the threads 
				interruptAll();
				
				try {
					for(Thread thread : componentExecutorThreads) {
						thread.join();
					}
					for(Thread thread : networkExecutorThreads) {
						thread.join();
					}
				} catch (InterruptedException e) {
					log.warn("threads may not have terminated");
				}
				
				try {
					tickThread.join();
					tickCounterThread.join();
				} catch (InterruptedException e) {
					log.warn("interrupted while waiting for tick thread");
				}
				
				// the local cluster can now be garbage collected
				synchronized(groupOperations) {
					for(GroupOp go : groupOperations.get(TermTopoGroupOp.class)) {
						node.localClusterTerminated((TermTopoGroupOp) go);
					}
				}
				
			}
		};
		shutdownThread.setName("shutdown");
		shutdownThread.start();
	}
	
	/**
	 * Startup a thread per component.
	 */
	private void runComponentThreads() {
		log.info("starting "+componentExecutorThreads.size()+" component executor threads");
		for(int i=0;i<componentExecutorThreads.size();i++){
			componentExecutorThreads.get(i).start();
		}
	}

	/**
	 * Check the output queues of components, processing network tasks found there and
	 * deliver tuples to the input queues of components.
	 */
	private void outputsScheduler(){
		log.debug("starting the outputs scheduler with "+conf.getDragonLocalclusterThreads()+" threads");
		for(int i=0;i<conf.getDragonLocalclusterThreads();i++) {
			final int me=i;
			networkExecutorThreads.add(new Thread() {
				@Override
				public void run(){
					log.info("starting up");
					HashSet<Integer> doneTaskIndices=new HashSet<Integer>();
					NetworkTaskBuffer queue;
					while(!isInterrupted()) {
						if(state==LocalCluster.State.HALTED) {
							log.info("halted");
							try {
								haltLock.lock();
								try {
									restartCondition.await();
								} catch (InterruptedException e) {
									log.info("interrupted");
									break;
								}
								log.info("resuming");
							} finally {
								haltLock.unlock();
							}
							continue;
						}
						
						try {
							queue = outputsPending[me].take();
						} catch (InterruptedException e1) {
							log.info("interrupted");
							break;
						}

						NetworkTask networkTask = (NetworkTask) queue.peek();
						while(networkTask!=null) {
							final Tuple[] tuples = networkTask.getTuples();
							final String name = networkTask.getComponentId();
							doneTaskIndices.clear();
							for(Integer taskIndex : networkTask.getTaskIndices()) {
								if(bolts.get(name).get(taskIndex).getInputCollector().getQueue().offer(tuples)){
									doneTaskIndices.add(taskIndex);
								} 
							}
							networkTask.getTaskIndices().removeAll(doneTaskIndices);
							if(networkTask.getTaskIndices().isEmpty()) {
								queue.poll();
								networkTask = (NetworkTask) queue.peek();
							} else {
								outputPending(queue);
								break;
							}
						}
					}
					log.info("shutting down");
				}
			});
			networkExecutorThreads.get(i).setName("netex "+i);
			networkExecutorThreads.get(i).start();
		}
		
	}
	
	/**
	 * Schedule a queue that has a new NetworkTask on it to be processed. There will
	 * be one reference of each queue for each NetworkTask that is outstanding on it.
	 * 
	 * @param queue the reference of the queue to process
	 */
	public void outputPending(final NetworkTaskBuffer queue) {
		// there is always enough space on this queue
		outputsPending[queue.hashCode()%networkExecutorThreads.size()].offer(queue);
	}
	
	/**
	 * 
	 * @return the name of the topology for this local cluster
	 */
	public String getTopologyId() {
		return topologyName;
	}
	
	/**
	 * 
	 * @return the topology conf for this local cluster
	 */
	public Config getConf(){
		return conf;
	}
	
	/**
	 * 
	 * @return the topology for this local cluster.
	 */
	public DragonTopology getTopology() {
		return dragonTopology;
	}

	/**
	 * Terminate the topology on this local cluster.
	 * @throws DragonInvalidStateException 
	 */
	public synchronized void setShouldTerminate() throws DragonInvalidStateException {
		/*
		 * Cannot terminate if we are already terminating or if we
		 * are in the fault state.
		 */
		if(state==State.TERMINATING) return;
		if(state==State.FAULT 
				|| state==State.ALLOCATED 
				|| state==State.SUBMITTED) throw new DragonInvalidStateException("cannot change to terminate state");
		log.info("terminating ["+getTopologyId()+"]");
		if(state==State.HALTED) {
			state=State.TERMINATING;
			try {
				haltLock.lock();
				restartCondition.signalAll();
			} finally {
				haltLock.unlock();
			}
		}
		state=State.TERMINATING;
		int index=0;
		for(HashMap<Integer,Spout> spouts : spouts.values()) {
			for(Spout component : spouts.values()) {
				component.setClosed();
				componentExecutorThreads.get(index).interrupt();
				try {
					component.close();
				} catch (Throwable e) {
					e.printStackTrace();
					log.error("throwable thrown by spout ["+component.getInstanceId()+"] when closing: "+e.getMessage());
				}
				log.debug(component.getInstanceId()+" closed");
				index++;
			}
		}
		checkCloseCondition();
	}
	
	/**
	 * Halt the topology on this local cluster.
	 * @throws DragonInvalidStateException 
	 */
	public void haltTopology() throws DragonInvalidStateException {
		if(state==State.HALTED) return;
		if(state!=State.RUNNING) throw new DragonInvalidStateException("must be running to halt the topology");
		log.info("halting ["+getTopologyId()+"]");
		state=State.HALTED;
	}
	
	/**
	 * Resume the topology from a halted state on this local cluster.
	 * @throws DragonInvalidStateException 
	 */
	public void resumeTopology() throws DragonInvalidStateException {
		if(state==State.HALTED) {
			log.info("resuming topology");
			state=State.RUNNING;
			try {
				haltLock.lock();
				restartCondition.signalAll();
			} finally {
				haltLock.unlock();
			}
		} else {
			throw new DragonInvalidStateException("can only resume from halted state");
		}
	}
	
	/**
	 * Set the topology into the FAULT state, which will require
	 * the topology to be purged and restarted. Generally a fault
	 * can happen at any time, e.g. if a node crashes that was running
	 * part of the topology, then the entire topology is at fault.
	 */
	public void setFault() {
		if(state==State.FAULT) return;
		log.info("topology fault has occurred");
		closeAll();
		interruptAll();
		state=State.FAULT;
		
	}
	
	/**
	 * Interrupt all threads.
	 */
	public void interruptAll() {
		if(state==State.FAULT) return;
		log.debug("interrupting all threads");
		if(state!=State.ALLOCATED && state!=State.SUBMITTED) {
			for(int i=0;i<componentExecutorThreads.size();i++) {
				componentExecutorThreads.get(i).interrupt();
			}
			for(int i=0;i<networkExecutorThreads.size();i++) {
				networkExecutorThreads.get(i).interrupt();
			}
		}
		tickThread.interrupt();
		tickCounterThread.interrupt();
	}
	
	/*
	 * Call all components' close(), specifically when purging, since
	 * this close operation is not safe.
	 */
	public void closeAll() {
		for(HashMap<Integer,Spout> spouts : spouts.values()) {
			for(Spout component : spouts.values()) {
				if(component.isClosing()!=true && component.isClosed()!=true) {
					try {
						component.setClosing();
						component.close();
						component.setClosed();
					} catch (Throwable e) {
						e.printStackTrace();
						log.error("throwable thrown by spout ["+component.getInstanceId()+"] when closing: "+e.getMessage());
					}
					log.debug(component.getInstanceId()+" closed");
				} else {
					log.debug(component.getInstanceId()+" already closed");
				}
			}
		}
		for(HashMap<Integer,Bolt> bolts : bolts.values()) {
			for(Bolt component : bolts.values()) {
				if(component.isClosing()!=true && component.isClosed()!=true) {
					try {
						component.setClosing();
						component.close();
						component.setClosed();
					} catch (Throwable e) {
						e.printStackTrace();
						log.error("throwable thrown by bolt ["+component.getInstanceId()+"] when closing: "+e.getMessage());
					}
					log.debug(component.getInstanceId()+" closed");
				} else {
					log.debug(component.getInstanceId()+" already closed");
				}
			}
		}
	}
	
	/**
	 * 
	 * @return the bolts that running on this local cluster.
	 */
	public HashMap<String,HashMap<Integer,Bolt>> getBolts(){
		return bolts;
	}
	
	/**
	 * 
	 * @return the spouts that are running on this local cluster.
	 */
	public HashMap<String,HashMap<Integer,Spout>> getSpouts(){
		return spouts;
	}
	
	/**
	 * 
	 * @return the node that this local cluster is attached to.
	 */
	public Node getNode(){
		return node;
	}
	
//	public CircularBlockingQueue<Component> getComponentsPending() {
//		return componentsPending;
//	}
	
	/**
	 * Record group operations that this local cluster needs to respond
	 * to, e.g. when the local cluster has terminated then it needs to
	 * respond to the terminate group operation.
	 * @param go
	 */
	public void setGroupOperation(GroupOp go) {
		synchronized(groupOperations) {
			if(!groupOperations.containsKey(go.getClass())) {
				groupOperations.put(go.getClass(), new HashSet<GroupOp>());
			}
			HashSet<GroupOp> gos = groupOperations.get(go.getClass());
			gos.add(go);
		}
	}

	/**
	 * Store up component exceptions and halt a topology if any component exceeds
	 * the set limit on exceptions. Stack traces are converted to a String.
	 * @param component ref to the component
	 * @param message the exception message
	 * @param stackTrace the stack trace of the exception
	 */
	public synchronized void componentException(Component component, String message, StackTraceElement[] stackTrace) {
		ComponentError ce = new ComponentError(message,stackTrace);
		if(!componentErrors.containsKey(component)) {
			componentErrors.put(component,new ArrayList<ComponentError>());
		}
		componentErrors.get(component).add(ce);
		int tolerance = conf.getDragonFaultsComponentTolerance();
		if(spoutConfs.containsKey(component.getComponentId())) {
			if(spoutConfs.get(component.getComponentId()).containsKey(Config.DRAGON_FAULTS_COMPONENT_TOLERANCE)) {
				tolerance=spoutConfs.get(component.getComponentId()).getDragonFaultsComponentTolerance();
			}
		}
		if(boltConfs.containsKey(component.getComponentId())) {
			if(boltConfs.get(component.getComponentId()).containsKey(Config.DRAGON_FAULTS_COMPONENT_TOLERANCE)) {
				tolerance=boltConfs.get(component.getComponentId()).getDragonFaultsComponentTolerance();
			}
		}
		if(componentErrors.get(component).size()>tolerance) {
			log.fatal("component ["+component.getInstanceId()
				+"] has failed more than ["+tolerance+"] times");
			if(node!=null) {
				if(state==LocalCluster.State.RUNNING) {
					node.signalHaltTopology(topologyName);
				}
			} else {
				System.exit(-1);
			}
		}
	}
	
	/**
	 * 
	 * @return the component errors accumulated for this local cluster
	 */
	public HashMap<Component, ArrayList<ComponentError>> getComponentErrors() {
		return componentErrors;
	}

	/**
	 * 
	 * @return the state of this local cluster
	 */
	public State getState() {
		return state;
	}

}
