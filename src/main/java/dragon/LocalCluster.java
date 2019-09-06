package dragon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.grouping.CustomStreamGrouping;
import dragon.network.Node;
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
import dragon.tuple.Tuple;
import dragon.tuple.Values;
import dragon.utils.CircularBuffer;


public class LocalCluster {
	private static Log log = LogFactory.getLog(LocalCluster.class);
	private HashMap<String,HashMap<Integer,Bolt>> bolts;
	private HashMap<String,HashMap<Integer,Spout>> spouts;
	private HashMap<String,Config> spoutConfs;
	private HashMap<String,Config> boltConfs;
	private ExecutorService componentExecutorService;
	private ExecutorService networkExecutorService;
	private int totalComponents=0;
	
	private String topologyName;
	private Config conf;
	private DragonTopology dragonTopology;
	
	private LinkedBlockingQueue<CircularBuffer<NetworkTask>> outputsPending;
	private LinkedBlockingQueue<Component> componentsPending;

	private Thread tickThread;
	private Thread tickCounterThread;
	private long tickTime=0;
	private long tickCounterTime=0;
	
	private HashMap<String,Integer> boltTickCount;
	
	int totalParallelismHint=0;
	
	private boolean shouldTerminate=false;
	
	private Node node;
	
	public LocalCluster(){
		
	}
	
	public LocalCluster(Node node) {
		this.node=node;
	}


	public void submitTopology(String topologyName, Config conf, DragonTopology dragonTopology) {
		this.topologyName=topologyName;
		this.conf=conf;
		this.dragonTopology=dragonTopology;
		outputsPending = new LinkedBlockingQueue<CircularBuffer<NetworkTask>>();
		componentsPending = new LinkedBlockingQueue<Component>();
		networkExecutorService = Executors.newFixedThreadPool((Integer)conf.get(Config.DRAGON_NETWORK_THREADS));
		
		
		
		// allocate spouts and open them
		spouts = new HashMap<String,HashMap<Integer,Spout>>();
		spoutConfs = new HashMap<String,Config>();
		for(String spoutId : dragonTopology.spoutMap.keySet()) {
			log.debug("allocating spout ["+spoutId+"]");
			spouts.put(spoutId, new HashMap<Integer,Spout>());
			HashMap<Integer,Spout> hm = spouts.get(spoutId);
			SpoutDeclarer spoutDeclarer = dragonTopology.spoutMap.get(spoutId);
			ArrayList<Integer> taskIds=new ArrayList<Integer>();
			totalParallelismHint+=spoutDeclarer.getParallelismHint();
			Map<String,Object> bc = spoutDeclarer.getSpout().getComponentConfiguration();
			Config spoutConf = new Config();
			spoutConf.putAll(bc);
			spoutConfs.put(spoutId, spoutConf);
			for(int i=0;i<spoutDeclarer.getNumTasks();i++) {
				taskIds.add(i);
			}
			for(int i=0;i<spoutDeclarer.getNumTasks();i++) {
				try {
					Spout spout=(Spout) spoutDeclarer.getSpout().clone();
					hm.put(i, spout);
					OutputFieldsDeclarer declarer = new OutputFieldsDeclarer();
					spout.declareOutputFields(declarer);
					spout.setOutputFieldsDeclarer(declarer);
					TopologyContext context = new TopologyContext(spoutId,i,taskIds);
					spout.setTopologyContext(context);
					spout.setLocalCluster(this);
					SpoutOutputCollector collector = new SpoutOutputCollector(this,spout);
					spout.setOutputCollector(collector);
					spout.open(conf, context, collector);
				} catch (CloneNotSupportedException e) {
					log.error("could not clone object: "+e.toString());
				}
			}
		}
		
		// allocate bolts and prepare them
		bolts = new HashMap<String,HashMap<Integer,Bolt>>();
		boltConfs = new HashMap<String,Config>();
		for(String boltId : dragonTopology.boltMap.keySet()) {
			log.debug("allocating bolt ["+boltId+"]");
			bolts.put(boltId, new HashMap<Integer,Bolt>());
			HashMap<Integer,Bolt> hm = bolts.get(boltId);
			BoltDeclarer boltDeclarer = dragonTopology.boltMap.get(boltId);
			totalParallelismHint+=boltDeclarer.getParallelismHint();
			Map<String,Object> bc = boltDeclarer.getBolt().getComponentConfiguration();
			Config boltConf = new Config();
			boltConf.putAll(bc);
			boltConfs.put(boltId, boltConf);
			ArrayList<Integer> taskIds=new ArrayList<Integer>();
			for(int i=0;i<boltDeclarer.getNumTasks();i++) {
				taskIds.add(i);
			}
			for(int i=0;i<boltDeclarer.getNumTasks();i++) {
				try {
					Bolt bolt=(Bolt) boltDeclarer.getBolt().clone();
					hm.put(i, bolt);
					OutputFieldsDeclarer declarer = new OutputFieldsDeclarer();
					bolt.declareOutputFields(declarer);
					bolt.setOutputFieldsDeclarer(declarer);
					TopologyContext context = new TopologyContext(boltId,i,taskIds);
					bolt.setTopologyContext(context);
					InputCollector inputCollector = new InputCollector(this, bolt);
					bolt.setInputCollector(inputCollector);
					bolt.setLocalCluster(this);
					OutputCollector collector = new OutputCollector(this,bolt);
					bolt.setOutputCollector(collector);
					bolt.prepare(conf, context, collector);
					
				} catch (CloneNotSupportedException e) {
					log.error("could not clone object: "+e.toString());
				}
			}
		}
		
		// prepare groupings
		for(String fromComponentId : dragonTopology.spoutMap.keySet()) {
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
				BoltDeclarer boltDeclarer = dragonTopology.boltMap.get(toComponentId);
				ArrayList<Integer> taskIds=new ArrayList<Integer>();
				for(int i=0;i<boltDeclarer.getNumTasks();i++) {
					taskIds.add(i);
				}
				for(String streamId : streams.keySet()) {
					HashSet<CustomStreamGrouping> groupings = dragonTopology.getGroupingsSet(fromComponentId, toComponentId, streamId);
					for(CustomStreamGrouping grouping : groupings) {
						grouping.prepare(null, null, taskIds);
					}
				}
			}
			
		}
		
		for(String fromComponentId : dragonTopology.boltMap.keySet()) {
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
				BoltDeclarer boltDeclarer = dragonTopology.boltMap.get(toComponentId);
				ArrayList<Integer> taskIds=new ArrayList<Integer>();
				for(int i=0;i<boltDeclarer.getNumTasks();i++) {
					taskIds.add(i);
				}
				for(String streamId : streams.keySet()) {
					GroupingsSet groupings = dragonTopology.getGroupingsSet(fromComponentId, toComponentId, streamId);
					for(CustomStreamGrouping grouping : groupings) {
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
				while(!shouldTerminate) {
					if(tickCounterTime==tickTime) {
						synchronized(tickCounterThread){
							try {
								wait();
							} catch (InterruptedException e) {
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
		tickCounterThread.start();
		
		tickThread = new Thread() {
			public void run() {
				while(!shouldTerminate) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						log.debug("terminating tick thread");
						break;
					}
					tickTime++;
					synchronized(tickCounterThread) {
						tickCounterThread.notify();
					}
				}
			}
		};
		tickThread.start();

		outputsScheduler();
		
		log.debug("starting a component executor with "+totalParallelismHint+" threads");
		componentExecutorService = Executors.newFixedThreadPool((Integer)totalParallelismHint);
		
		log.debug("scheduling spouts to run");
		for(String componentId : spouts.keySet()) {
			HashMap<Integer,Spout> component = spouts.get(componentId);
			for(Integer taskId : component.keySet()) {
				Spout spout = component.get(taskId);
				componentPending(spout);
			}
		}
		
		runComponentThreads();
		
		
	}
	
	private void issueTickTuple(String boltId) {
		Tuple tuple=new Tuple(new Fields("tick"));
		tuple.setValues(new Values("0"));
		tuple.setSourceComponent(Constants.SYSTEM_COMPONENT_ID);
		tuple.setSourceStreamId(Constants.SYSTEM_TICK_STREAM_ID);
		for(Bolt bolt : bolts.get(boltId).values()) {
			synchronized(bolt) {
				bolt.setTickTuple(tuple);
				componentPending(bolt);
			}
		}
	}
	
	private void runComponentThreads() {
		for(int i=0;i<totalParallelismHint;i++){
			componentExecutorService.execute(new Runnable(){
				public void run(){
					while(!shouldTerminate){
						Component component;
						try {
							component = componentsPending.take();
						} catch (InterruptedException e) {
							break;
						}
						// TODO:the synchronization can be switched if the component's
						// execute/nextTuple is thread safe
						synchronized(component) {
							component.run();
						}	
					}
				}
			});
		}
	}

	
	private void outputsScheduler(){
		log.debug("starting the outputs scheduler with "+(Integer)conf.get(Config.DRAGON_NETWORK_THREADS)+" threads");
		for(int i=0;i<(Integer)conf.get(Config.DRAGON_NETWORK_THREADS);i++) {
			networkExecutorService.execute(new Runnable() {
				public void run(){
					HashSet<Integer> doneTaskIds=new HashSet<Integer>();
					while(!shouldTerminate) {
						try {
							CircularBuffer<NetworkTask> queue = outputsPending.take();
							//log.debug("network task pending "+outputsPending.size());
							synchronized(queue.lock) {
								//log.debug("peeking on queue");
								NetworkTask networkTask = (NetworkTask) queue.peek();
								if(networkTask!=null) {
									//log.debug("processing network task");
									Tuple tuple = networkTask.getTuple();
									String name = networkTask.getComponentId();
									doneTaskIds.clear();
									for(Integer taskId : networkTask.getTaskIds()) {
										if(bolts.get(name).get(taskId).getInputCollector().getQueue().offer(tuple)){
											//log.debug("removing from queue");
											doneTaskIds.add(taskId);
											componentPending(bolts.get(name).get(taskId));
										} else {
											//log.debug("blocked");
										}
									}
									networkTask.getTaskIds().removeAll(doneTaskIds);
									if(networkTask.getTaskIds().size()==0) {
										queue.poll();
									} else {
										outputPending(queue);
									}
								} else {
									//log.debug("queue empty!");
								}
							}
						} catch (InterruptedException e) {
							break;
						}
					}
				}
			});
		}
		
	}
	
	public void componentPending(final Component component){
		try {
			componentsPending.put(component);
		} catch (InterruptedException e){
			log.error("interruptep while adding component pending");
		}
	}
	
	public void outputPending(final CircularBuffer<NetworkTask> queue) {
		try {
			outputsPending.put(queue);
			//log.debug("outputPending pending "+outputsPending.size());
		} catch (InterruptedException e) {
			log.error("interruptep while adding output pending");
		}
	}
	
	public String getPersistanceDir(){
		return conf.get(Config.DRAGON_BASE_DIR)+"/"+conf.get(Config.DRAGON_PERSISTANCE_DIR);
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
		shouldTerminate=true;
	}
	
	public void setShouldTerminate(String error) {
		setShouldTerminate();
		throw new RuntimeException(error);
	}
	
	public HashMap<String,HashMap<Integer,Bolt>> getBolts(){
		return bolts;
	}
	
	public Node getNode(){
		return node;
	}
	
}
