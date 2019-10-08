package dragon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.grouping.AbstractGrouping;
import dragon.network.Node;
import dragon.network.operations.GroupOperation;
import dragon.network.operations.TerminateTopologyGroupOperation;
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
import dragon.tuple.RecycleStation;
import dragon.tuple.Tuple;
import dragon.tuple.Values;
import dragon.utils.CircularBlockingQueue;
import dragon.utils.NetworkTaskBuffer;


public class LocalCluster {
	private final static Log log = LogFactory.getLog(LocalCluster.class);
	private HashMap<String,HashMap<Integer,Bolt>> bolts;
	private HashMap<String,HashMap<Integer,Spout>> spouts;
	private HashMap<String,Config> spoutConfs;
	private HashMap<String,Config> boltConfs;
	private ArrayList<Thread> componentExecutorThreads;
	private ArrayList<Thread> networkExecutorThreads;
	private HashMap<Component,ArrayList<ComponentError>> componentErrors;
	

	public static enum State {
		ALLOCATED,
		SUBMITTED,
		RUNNING,
		TERMINATING,
		HALTED
	}
	
	private volatile State state;
	
	private final ReentrantLock haltLock = new ReentrantLock();
	private final Condition restartCondition = haltLock.newCondition();
	
	@SuppressWarnings("rawtypes")
	private HashMap<Class,HashSet<GroupOperation>> groupOperations;
	
	private String topologyName;
	private Config conf;
	private DragonTopology dragonTopology;
	
	private CircularBlockingQueue<NetworkTaskBuffer> outputsPending;
	private CircularBlockingQueue<Component> componentsPending;

	private Thread tickThread;
	private Thread tickCounterThread;
	private long tickTime=0;
	private long tickCounterTime=0;
	
	private HashMap<String,Integer> boltTickCount;
	
	private int totalParallelismHint=0;
	
	private final Node node;
	
	private final Fields tickFields=new Fields("tick");
	

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
	
	private ArrayList<BoltPrepare> boltPrepareList;
	
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
	
	private ArrayList<SpoutOpen> spoutOpenList;
	
	@SuppressWarnings("rawtypes")
	public LocalCluster(){
		this.node=null;
		state=State.ALLOCATED;
		groupOperations = new HashMap<Class,HashSet<GroupOperation>>();
		componentErrors = new  HashMap<Component,ArrayList<ComponentError>>();
	}
	
	@SuppressWarnings("rawtypes")
	public LocalCluster(Node node) {
		this.node=node;
		state=State.ALLOCATED;
		groupOperations = new HashMap<Class,HashSet<GroupOperation>>();
		componentErrors = new  HashMap<Component,ArrayList<ComponentError>>();
	}
	
	public void submitTopology(String topologyName, Config conf, DragonTopology dragonTopology) {
		Config lconf=null;
		try {
			lconf = new Config(Constants.DRAGON_PROPERTIES);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		lconf.putAll(conf);
		try {
			submitTopology(topologyName, lconf, dragonTopology, true);
		} catch (DragonRequiresClonableException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void submitTopology(String topologyName, Config conf, DragonTopology dragonTopology, boolean start) throws DragonRequiresClonableException {
		this.topologyName=topologyName;
		this.conf=conf;
		this.dragonTopology=dragonTopology;
		
		
		//networkExecutorService = (ThreadPoolExecutor) Executors.newFixedThreadPool((Integer)conf.getDragonLocalclusterThreads());
		networkExecutorThreads = new ArrayList<Thread>();
		if(!start) {
			spoutOpenList = new ArrayList<SpoutOpen>();
			boltPrepareList = new ArrayList<BoltPrepare>();
		}
		
		RecycleStation.getInstance().createTupleRecycler(new Tuple(tickFields));
		
		int totalOutputsBufferSize=0;
		int totalInputsBufferSize=0;
		
		// allocate spouts and open them
		spouts = new HashMap<String,HashMap<Integer,Spout>>();
		spoutConfs = new HashMap<String,Config>();
		for(String spoutId : dragonTopology.getSpoutMap().keySet()) {
			boolean localcomponent = !(dragonTopology.getReverseEmbedding()!=null &&
					!dragonTopology.getReverseEmbedding().contains(node.getComms().getMyNodeDescriptor(),spoutId));
			HashMap<Integer,Spout> hm=null;
			SpoutDeclarer spoutDeclarer = dragonTopology.getSpoutMap().get(spoutId);
			ArrayList<Integer> taskIds=new ArrayList<Integer>();
			for(int i=0;i<spoutDeclarer.getNumTasks();i++) {
				taskIds.add(i);
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
				boolean localtask = !(dragonTopology.getReverseEmbedding()!=null &&
						!dragonTopology.getReverseEmbedding().contains(node.getComms().getMyNodeDescriptor(),spoutId,i));
				try {
					if(localtask) numAllocated++;
					Spout spout=(Spout) spoutDeclarer.getSpout().clone();
					if(localtask) hm.put(i, spout);
					OutputFieldsDeclarer declarer = new OutputFieldsDeclarer();
					spout.declareOutputFields(declarer);
					spout.setOutputFieldsDeclarer(declarer);
					TopologyContext context = new TopologyContext(spoutId,i,taskIds);
					spout.setTopologyContext(context);
					spout.setLocalCluster(this);
					SpoutOutputCollector collector = new SpoutOutputCollector(this,spout);
					if(localtask) totalOutputsBufferSize+=collector.getTotalBufferSpace();
					spout.setOutputCollector(collector);
					if(localtask) {
						if(start) {
							spout.open(conf, context, collector);
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
					!dragonTopology.getReverseEmbedding().contains(node.getComms().getMyNodeDescriptor(),boltId));
			HashMap<Integer,Bolt> hm=null;
			BoltDeclarer boltDeclarer = dragonTopology.getBoltMap().get(boltId);
			ArrayList<Integer> taskIds=new ArrayList<Integer>();
			for(int i=0;i<boltDeclarer.getNumTasks();i++) {
				taskIds.add(i);
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
						!dragonTopology.getReverseEmbedding().contains(node.getComms().getMyNodeDescriptor(),boltId,i));
				try {
					if(localtask) numAllocated++;
					Bolt bolt=(Bolt) boltDeclarer.getBolt().clone();
					if(localtask) hm.put(i, bolt);
					OutputFieldsDeclarer declarer = new OutputFieldsDeclarer();
					bolt.declareOutputFields(declarer);
					bolt.setOutputFieldsDeclarer(declarer);
					TopologyContext context = new TopologyContext(boltId,i,taskIds);
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
							bolt.prepare(conf, context, collector);
						} else {
							prepareLater(bolt,context,collector);
						}
					}
				} catch (CloneNotSupportedException e) {
					log.error("could not clone object: "+e.toString());
				}
			}
			totalParallelismHint+=Math.ceil((double)numAllocated*boltDeclarer.getParallelismHint()/boltDeclarer.getNumTasks());
		}
		
		log.info("total outputs buffer size is "+totalOutputsBufferSize);
		outputsPending = new CircularBlockingQueue<NetworkTaskBuffer>(totalOutputsBufferSize);
		log.info("total inputs buffer size is "+totalInputsBufferSize);
		componentsPending = new CircularBlockingQueue<Component>(totalInputsBufferSize);
		
		
		// prepare groupings
		for(String fromComponentId : dragonTopology.getSpoutMap().keySet()) {
			log.debug("preparing groupings for spout["+fromComponentId+"]");
			HashMap<String,StreamMap> 
				fromComponent = dragonTopology.getDestComponentMap(fromComponentId);
			if(fromComponent==null) {
				throw new RuntimeException("spout ["+fromComponentId+"] has no components listening to it");
			}
			log.debug(fromComponent);
			for(String toComponentId : fromComponent.keySet()) {
				HashMap<String,GroupingsSet> 
					streams = dragonTopology.getStreamMap(fromComponentId, toComponentId);
				BoltDeclarer boltDeclarer = dragonTopology.getBoltMap().get(toComponentId);
				ArrayList<Integer> taskIds=new ArrayList<Integer>();
				for(int i=0;i<boltDeclarer.getNumTasks();i++) {
					taskIds.add(i);
				}
				for(String streamId : streams.keySet()) {
					GroupingsSet groupings = dragonTopology.getGroupingsSet(fromComponentId, toComponentId, streamId);
					for(AbstractGrouping grouping : groupings) {
						grouping.prepare(null, null, taskIds);
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
				ArrayList<Integer> taskIds=new ArrayList<Integer>();
				for(int i=0;i<boltDeclarer.getNumTasks();i++) {
					taskIds.add(i);
				}
				for(String streamId : streams.keySet()) {
					GroupingsSet groupings = dragonTopology.getGroupingsSet(fromComponentId, toComponentId, streamId);
					for(AbstractGrouping grouping : groupings) {
						grouping.prepare(null, null, taskIds);
					}
				}
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


		outputsScheduler();
		
		componentExecutorThreads = new ArrayList<Thread>();
		state=State.SUBMITTED;
		if(start) {
			scheduleSpouts();	
		}
	}
	
	private void scheduleSpouts() {
		log.debug("scheduling spouts to run");
		for(String componentId : spouts.keySet()) {
			HashMap<Integer,Spout> component = spouts.get(componentId);
			for(Integer taskId : component.keySet()) {
				Spout spout = component.get(taskId);
				componentPending(spout);
			}
		}
		runComponentThreads();
		state=State.RUNNING;
	}
	
	private void prepareLater(Bolt bolt, TopologyContext context, OutputCollector collector) {
		boltPrepareList.add(new BoltPrepare(bolt,context,collector));
	}
	
	private void openLater(Spout spout, TopologyContext context, SpoutOutputCollector collector) {
		spoutOpenList.add(new SpoutOpen(spout,context,collector));
	}
	
	public void openAll() {
		log.info("opening bolts");
		for(BoltPrepare boltPrepare : boltPrepareList) {
			//log.debug("calling prepare on bolt "+boltPrepare+" with collector "+boltPrepare.collector);
			boltPrepare.bolt.prepare(conf, boltPrepare.context, boltPrepare.collector);
		}
		for(SpoutOpen spoutOpen : spoutOpenList) {
			//log.debug("calling open on spout "+spoutOpen.spout+" with collector "+spoutOpen.collector);
			spoutOpen.spout.open(conf, spoutOpen.context, spoutOpen.collector);
		}
		log.debug("scheduling spouts");
		scheduleSpouts();
	}

	private void issueTickTuple(String boltId) {
		Tuple tuple=RecycleStation.getInstance().getTupleRecycler(tickFields.getFieldNamesAsString()).newObject();;
		tuple.setValues(new Values("0"));
		tuple.setSourceComponent(Constants.SYSTEM_COMPONENT_ID);
		tuple.setSourceStreamId(Constants.SYSTEM_TICK_STREAM_ID);
		for(Bolt bolt : bolts.get(boltId).values()) {
			synchronized(bolt) {
				RecycleStation.getInstance().getTupleRecycler(tickFields.getFieldNamesAsString()).shareRecyclable(tuple, 1);
				bolt.setTickTuple(tuple);
				componentPending(bolt);
			}
		}
		RecycleStation.getInstance().getTupleRecycler(tickFields.getFieldNamesAsString()).crushRecyclable(tuple, 1);
	}
	
	private void checkCloseCondition() {
		log.debug("starting shutdown thread");
		Thread shutdownThread = new Thread() {
			@Override
			public void run() {
				// wait for spouts to close
				while(true) {
					boolean spoutsClosed=true;
					for(String componentId : spouts.keySet()) {
						HashMap<Integer,Spout> component = spouts.get(componentId);
						for(Integer taskId : component.keySet()) {
							Spout spout = component.get(taskId);
							if(!spout.isClosed()) {
								spoutsClosed=false;
								break;
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
				for(String componentId : spouts.keySet()) {
					HashMap<Integer,Spout> component = spouts.get(componentId);
					for(Integer taskId : component.keySet()) {
						Spout spout = component.get(taskId);
						spout.getOutputCollector().emitTerminateTuple();
					}
				}
				
				// wait for bolts to close (they only close when they receive the
				// terminate tuple on all of their input streams, from all tasks on
				// those streams).
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
				log.debug("interrupting all threads");
				for(int i=0;i<totalParallelismHint;i++) {
					componentExecutorThreads.get(i).interrupt();
				}
				for(int i=0;i<conf.getDragonLocalclusterThreads();i++) {
					networkExecutorThreads.get(i).interrupt();
				}
				
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
				
				tickThread.interrupt();
				tickCounterThread.interrupt();
				
				try {
					tickThread.join();
					tickCounterThread.join();
				} catch (InterruptedException e) {
					log.warn("interrupted while waiting for tick thread");
				}
				
				// the local cluster can now be garbage collected
				synchronized(groupOperations) {
					for(GroupOperation go : groupOperations.get(TerminateTopologyGroupOperation.class)) {
						node.localClusterTerminated(topologyName,(TerminateTopologyGroupOperation) go);
					}
				}
				
			}
		};
		shutdownThread.setName("shutdown");
		shutdownThread.start();
	}
	
	private void runComponentThreads() {
		log.info("starting component executor threads with "+totalParallelismHint+" threads");
		for(int i=0;i<totalParallelismHint;i++){
			//final int thread_id=i;
			componentExecutorThreads.add(new Thread(){
				@Override
				public void run(){
					Component component;
					while(!isInterrupted()){
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
							continue;
						}
						try {
							component = componentsPending.take();
						} catch (InterruptedException e) {
							log.info(getName()+" interrupted");
							break;
						}
						
						component.run();
							
					}
					log.info(getName()+" done");
				}
			});
			componentExecutorThreads.get(i).setName("component executor "+i);
			componentExecutorThreads.get(i).start();
		}
	}

	
	private void outputsScheduler(){
		log.debug("starting the outputs scheduler with "+conf.getDragonLocalclusterThreads()+" threads");
		for(int i=0;i<conf.getDragonLocalclusterThreads();i++) {
			networkExecutorThreads.add(new Thread() {
				@Override
				public void run(){
					HashSet<Integer> doneTaskIds=new HashSet<Integer>();
					NetworkTaskBuffer queue;
					while(!isInterrupted()) {
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
							continue;
						}
						try {
							queue = outputsPending.take();
						} catch (InterruptedException e) {
							log.info(getName()+" interrupted");
							break;
						}
						// while the queue is thread safe, we lock on it because of the use of peek
						queue.bufferLock.lock();
						try {
							//if(shouldTerminate) System.out.println("count "+queue.size());
							NetworkTask networkTask = (NetworkTask) queue.peek();
							if(networkTask!=null) {
								//System.out.println("processing task");
								Tuple tuple = networkTask.getTuple();
								String name = networkTask.getComponentId();
								doneTaskIds.clear();
								for(Integer taskId : networkTask.getTaskIds()) {
									RecycleStation.getInstance()
									.getTupleRecycler(tuple.getFields()
											.getFieldNamesAsString()).shareRecyclable(tuple,1);
									if(bolts.get(name).get(taskId).getInputCollector().getQueue().offer(tuple)){
										doneTaskIds.add(taskId);
										componentPending(bolts.get(name).get(taskId));
									} else {
										RecycleStation.getInstance()
										.getTupleRecycler(tuple.getFields()
												.getFieldNamesAsString()).crushRecyclable(tuple,1);
										//log.debug("blocked");
									}
								}
								networkTask.getTaskIds().removeAll(doneTaskIds);
								if(networkTask.getTaskIds().size()==0) {
									queue.poll();
									RecycleStation.getInstance().getNetworkTaskRecycler().crushRecyclable(networkTask, 1);
								} else {
									outputPending(queue);
								}
							} else {
								log.error(getName()+" queue empty!");
							}	
						} finally {
							queue.bufferLock.unlock();
						}
					}
					log.info(getName()+" done");
				}
			});
			networkExecutorThreads.get(i).setName("network executor "+i);
			networkExecutorThreads.get(i).start();
		}
		
	}
	
	public void componentPending(final Component component){
		try {
			componentsPending.put(component);
		} catch (InterruptedException e){
			log.error("interrupted while adding component pending ["+component.getComponentId()+":"+component.getTaskId()+"]");
		}
	}
	
	public void outputPending(final NetworkTaskBuffer queue) {
		try {
			outputsPending.put(queue);
			//log.debug("outputPending pending "+outputsPending.size());
		} catch (InterruptedException e) {
			log.error("interrupted while adding output pending");
		}
	}
	
	public String getTopologyId() {
		return topologyName;
	}
	
	public Config getConf(){
		return conf;
	}
	
	public DragonTopology getTopology() {
		return dragonTopology;
	}

	public void setShouldTerminate() {
		if(state==State.TERMINATING) return;
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
		for(String componentId : spouts.keySet()) {
			HashMap<Integer,Spout> component = spouts.get(componentId);
			for(Integer taskId : component.keySet()) {
				Spout spout = component.get(taskId);
				spout.setClosing();
				spout.run();
			}
		}
		checkCloseCondition();
	}
	
	public void haltTopology() {
		log.info("halting topology");
		state=State.HALTED;
	}
	
	public void resumeTopology() {
		log.info("resuming topology");
		if(state==State.HALTED) {
			state=State.RUNNING;
			try {
				haltLock.lock();
				restartCondition.signalAll();
			} finally {
				haltLock.unlock();
			}
		}
	}
	
	public HashMap<String,HashMap<Integer,Bolt>> getBolts(){
		return bolts;
	}
	
	public HashMap<String,HashMap<Integer,Spout>> getSpouts(){
		return spouts;
	}
	
	public Node getNode(){
		return node;
	}
	
	public void setGroupOperation(GroupOperation go) {
		synchronized(groupOperations) {
			if(!groupOperations.containsKey(go.getClass())) {
				groupOperations.put(go.getClass(), new HashSet<GroupOperation>());
			}
			HashSet<GroupOperation> gos = groupOperations.get(go.getClass());
			gos.add(go);
		}
	}

	public void componentException(Component component, String message, StackTraceElement[] stackTrace) {
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
			log.fatal("component ["+component.getComponentId()+"] has failed more than ["+tolerance+"] times");
			if(state==LocalCluster.State.RUNNING)node.signalHaltTopology(topologyName);
		}
	}
	
	public HashMap<Component, ArrayList<ComponentError>> getComponentErrors() {
		return componentErrors;
	}

	public State getState() {
		return state;
	}


}
