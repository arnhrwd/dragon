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
import dragon.utils.NetworkTaskBuffer;


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
	
	private LinkedBlockingQueue<NetworkTaskBuffer> outputsPending;
	private LinkedBlockingQueue<Component> componentsPending;

	private Thread tickThread;
	private Thread tickCounterThread;
	private long tickTime=0;
	private long tickCounterTime=0;
	
	private HashMap<String,Integer> boltTickCount;
	
	int totalParallelismHint=0;
	
	private boolean shouldTerminate=false;
	
	private Node node;
	

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
	
	public LocalCluster(){
		
	}
	
	public LocalCluster(Node node) {
		this.node=node;
	}
	
	public void submitTopology(String topologyName, Config conf, DragonTopology dragonTopology) {
		submitTopology(topologyName, conf, dragonTopology, true);
	}

	public void submitTopology(String topologyName, Config conf, DragonTopology dragonTopology, boolean start) {
		this.topologyName=topologyName;
		this.conf=conf;
		this.dragonTopology=dragonTopology;
		outputsPending = new LinkedBlockingQueue<NetworkTaskBuffer>();
		componentsPending = new LinkedBlockingQueue<Component>();
		networkExecutorService = Executors.newFixedThreadPool((Integer)conf.get(Config.DRAGON_LOCALCLUSTER_THREADS));
		
		if(!start) {
			spoutOpenList = new ArrayList<SpoutOpen>();
			boltPrepareList = new ArrayList<BoltPrepare>();
		}
		
		// allocate spouts and open them
		spouts = new HashMap<String,HashMap<Integer,Spout>>();
		spoutConfs = new HashMap<String,Config>();
		for(String spoutId : dragonTopology.getSpoutMap().keySet()) {
			if(dragonTopology.getReverseEmbedding()!=null &&
					!dragonTopology.getReverseEmbedding().contains(node.getComms().getMyNodeDescriptor(),spoutId)) continue;
			log.debug("allocating spout ["+spoutId+"]");
			spouts.put(spoutId, new HashMap<Integer,Spout>());
			HashMap<Integer,Spout> hm = spouts.get(spoutId);
			SpoutDeclarer spoutDeclarer = dragonTopology.getSpoutMap().get(spoutId);
			ArrayList<Integer> taskIds=new ArrayList<Integer>();
			Map<String,Object> bc = spoutDeclarer.getSpout().getComponentConfiguration();
			Config spoutConf = new Config();
			spoutConf.putAll(bc);
			spoutConfs.put(spoutId, spoutConf);
			for(int i=0;i<spoutDeclarer.getNumTasks();i++) {
				taskIds.add(i);
			}
			int numAllocated=0;
			for(int i=0;i<spoutDeclarer.getNumTasks();i++) {
				if(dragonTopology.getReverseEmbedding()!=null &&
						!dragonTopology.getReverseEmbedding().contains(node.getComms().getMyNodeDescriptor(),spoutId,i)) continue;
				numAllocated++;
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
					if(start) {
						spout.open(conf, context, collector);
					} else {
						openLater(spout,context,collector);
					}
				} catch (CloneNotSupportedException e) {
					setShouldTerminate("could not clone object: "+e.toString());
				}
			}
			// TODO: make use of parallelism hint from topology to possibly reduce threads
			totalParallelismHint+=numAllocated;
		}
		
		// allocate bolts and prepare them
		bolts = new HashMap<String,HashMap<Integer,Bolt>>();
		boltConfs = new HashMap<String,Config>();
		for(String boltId : dragonTopology.getBoltMap().keySet()) {
			if(dragonTopology.getReverseEmbedding()!=null &&
					!dragonTopology.getReverseEmbedding().contains(node.getComms().getMyNodeDescriptor(),boltId)) continue;
			log.debug("allocating bolt ["+boltId+"]");
			bolts.put(boltId, new HashMap<Integer,Bolt>());
			HashMap<Integer,Bolt> hm = bolts.get(boltId);
			BoltDeclarer boltDeclarer = dragonTopology.getBoltMap().get(boltId);
			Map<String,Object> bc = boltDeclarer.getBolt().getComponentConfiguration();
			Config boltConf = new Config();
			boltConf.putAll(bc);
			boltConfs.put(boltId, boltConf);
			ArrayList<Integer> taskIds=new ArrayList<Integer>();
			for(int i=0;i<boltDeclarer.getNumTasks();i++) {
				taskIds.add(i);
			}
			int numAllocated=0;
			for(int i=0;i<boltDeclarer.getNumTasks();i++) {
				if(dragonTopology.getReverseEmbedding()!=null &&
						!dragonTopology.getReverseEmbedding().contains(node.getComms().getMyNodeDescriptor(),boltId,i)) continue;
				numAllocated++;
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
					if(start) {
						bolt.prepare(conf, context, collector);
					} else {
						prepareLater(bolt,context,collector);
					}
				} catch (CloneNotSupportedException e) {
					log.error("could not clone object: "+e.toString());
				}
			}
			totalParallelismHint+=numAllocated;
		}
		
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
					HashSet<CustomStreamGrouping> groupings = dragonTopology.getGroupingsSet(fromComponentId, toComponentId, streamId);
					for(CustomStreamGrouping grouping : groupings) {
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
		
		if(start) {
			scheduleSpouts();
		}
		
		runComponentThreads();
		
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
	}
	
	private void prepareLater(Bolt bolt, TopologyContext context, OutputCollector collector) {
		boltPrepareList.add(new BoltPrepare(bolt,context,collector));
	}
	
	private void openLater(Spout spout, TopologyContext context, SpoutOutputCollector collector) {
		spoutOpenList.add(new SpoutOpen(spout,context,collector));
	}
	
	public void openAll() {
		for(BoltPrepare boltPrepare : boltPrepareList) {
			boltPrepare.bolt.prepare(conf, boltPrepare.context, boltPrepare.collector);
		}
		for(SpoutOpen spoutOpen : spoutOpenList) {
			spoutOpen.spout.open(conf, spoutOpen.context, spoutOpen.collector);
		}
		scheduleSpouts();
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
		log.debug("starting the outputs scheduler with "+(Integer)conf.get(Config.DRAGON_LOCALCLUSTER_THREADS)+" threads");
		for(int i=0;i<(Integer)conf.get(Config.DRAGON_LOCALCLUSTER_THREADS);i++) {
			networkExecutorService.execute(new Runnable() {
				public void run(){
					HashSet<Integer> doneTaskIds=new HashSet<Integer>();
					while(!shouldTerminate) {
						try {
							NetworkTaskBuffer queue = outputsPending.take();
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
	
	public void outputPending(final NetworkTaskBuffer queue) {
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
	
	public HashMap<String,HashMap<Integer,Spout>> getSpouts(){
		return spouts;
	}
	
	public Node getNode(){
		return node;
	}
	
}
