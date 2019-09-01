package dragon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.grouping.CustomStreamGrouping;
import dragon.spout.SpoutOutputCollector;
import dragon.task.InputCollector;
import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.BoltDeclarer;
import dragon.topology.DragonTopology;
import dragon.topology.OutputFieldsDeclarer;
import dragon.topology.SpoutDeclarer;
import dragon.topology.base.Collector;
import dragon.topology.base.IRichBolt;
import dragon.topology.base.IRichSpout;
import dragon.tuple.Tuple;


public class LocalCluster {
	private Log log = LogFactory.getLog(LocalCluster.class);
	private HashMap<String,HashMap<Integer,IRichBolt>> iRichBolts;
	private HashMap<String,HashMap<Integer,IRichSpout>> iRichSpouts;
	private ExecutorService componentExecutorService;
	private ExecutorService networkExecutorService;
	
	private String topologyName;
	private Config conf;
	private DragonTopology dragonTopology;
	
	private LinkedBlockingQueue<Collector> outputsPending;
	
	private boolean shouldTerminate=false;
	
	public LocalCluster() {
		
	}

	public void submitTopology(String topologyName, Config conf, DragonTopology dragonTopology) {
		this.topologyName=topologyName;
		this.conf=conf;
		this.dragonTopology=dragonTopology;
		outputsPending = new LinkedBlockingQueue<Collector>();
		networkExecutorService = Executors.newFixedThreadPool((Integer)conf.get(Config.DRAGON_NETWORK_THREADS));
		
		int totalParallelismHint=0;
		
		// allocate spouts and open them
		iRichSpouts = new HashMap<String,HashMap<Integer,IRichSpout>>();
		for(String spoutId : dragonTopology.spoutMap.keySet()) {
			iRichSpouts.put(spoutId, new HashMap<Integer,IRichSpout>());
			HashMap<Integer,IRichSpout> hm = iRichSpouts.get(spoutId);
			SpoutDeclarer spoutDeclarer = dragonTopology.spoutMap.get(spoutId);
			ArrayList<Integer> taskIds=new ArrayList<Integer>();
			totalParallelismHint+=spoutDeclarer.getParallelismHint();
			for(int i=0;i<spoutDeclarer.getNumTasks();i++) {
				taskIds.add(i);
			}
			for(int i=0;i<spoutDeclarer.getNumTasks();i++) {
				try {
					IRichSpout spout=(IRichSpout) spoutDeclarer.getSpout().clone();
					hm.put(i, spout);
					OutputFieldsDeclarer declarer = new OutputFieldsDeclarer();
					spout.declareOutputFields(declarer);
					spout.setOutputFieldsDeclarer(declarer);
					TopologyContext context = new TopologyContext(spoutId,i,taskIds);
					spout.setTopologyContext(context);
					spout.setLocalCluster(this);
					SpoutOutputCollector collector = new SpoutOutputCollector(this,spout);
					spout.open(conf, context, collector);
				} catch (CloneNotSupportedException e) {
					log.error("could not clone object: "+e.toString());
				}
			}
		}
		
		// allocate bolts and open them
		iRichBolts = new HashMap<String,HashMap<Integer,IRichBolt>>();
		for(String boltId : dragonTopology.boltMap.keySet()) {
			iRichBolts.put(boltId, new HashMap<Integer,IRichBolt>());
			HashMap<Integer,IRichBolt> hm = iRichBolts.get(boltId);
			BoltDeclarer boltDeclarer = dragonTopology.boltMap.get(boltId);
			totalParallelismHint+=boltDeclarer.getParallelismHint();
			ArrayList<Integer> taskIds=new ArrayList<Integer>();
			for(int i=0;i<boltDeclarer.getNumTasks();i++) {
				taskIds.add(i);
			}
			for(int i=0;i<boltDeclarer.getNumTasks();i++) {
				try {
					IRichBolt bolt=(IRichBolt) boltDeclarer.getBolt().clone();
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
					bolt.prepare(conf, context, collector);
					
				} catch (CloneNotSupportedException e) {
					log.error("could not clone object: "+e.toString());
				}
			}
		}
		
		// prepare groupings
		for(String fromComponentId : dragonTopology.spoutMap.keySet()) {
			log.debug("preparing groupings for spout["+fromComponentId+"]");
			HashMap<String,HashMap<String,HashSet<CustomStreamGrouping>>> 
				fromComponent = dragonTopology.getFromComponent(fromComponentId);
			log.debug(fromComponent);
			for(String toComponentId : fromComponent.keySet()) {
				HashMap<String,HashSet<CustomStreamGrouping>> 
					streams = dragonTopology.getFromToComponent(fromComponentId, toComponentId);
				BoltDeclarer boltDeclarer = dragonTopology.boltMap.get(toComponentId);
				ArrayList<Integer> taskIds=new ArrayList<Integer>();
				for(int i=0;i<boltDeclarer.getNumTasks();i++) {
					taskIds.add(i);
				}
				for(String streamId : streams.keySet()) {
					HashSet<CustomStreamGrouping> groupings = dragonTopology.getFromToStream(fromComponentId, toComponentId, streamId);
					for(CustomStreamGrouping grouping : groupings) {
						grouping.prepare(null, null, taskIds);
					}
				}
			}
			
		}
		
		for(String fromComponentId : dragonTopology.boltMap.keySet()) {
			log.debug("preparing groupings for bolt["+fromComponentId+"]");
			HashMap<String,HashMap<String,HashSet<CustomStreamGrouping>>> 
				fromComponent = dragonTopology.getFromComponent(fromComponentId);
			if(fromComponent==null) {
				log.debug(fromComponentId+" is a sink");
				continue;
			}
			for(String toComponentId : fromComponent.keySet()) {
				HashMap<String,HashSet<CustomStreamGrouping>> 
					streams = dragonTopology.getFromToComponent(fromComponentId, toComponentId);
				BoltDeclarer boltDeclarer = dragonTopology.boltMap.get(toComponentId);
				ArrayList<Integer> taskIds=new ArrayList<Integer>();
				for(int i=0;i<boltDeclarer.getNumTasks();i++) {
					taskIds.add(i);
				}
				for(String streamId : streams.keySet()) {
					HashSet<CustomStreamGrouping> groupings = dragonTopology.getFromToStream(fromComponentId, toComponentId, streamId);
					for(CustomStreamGrouping grouping : groupings) {
						grouping.prepare(null, null, taskIds);
					}
				}
			}
			
		}
		
		outputsScheduler();
		
		log.debug("starting a component executor with "+totalParallelismHint+" threads");
		componentExecutorService = Executors.newFixedThreadPool((Integer)totalParallelismHint);
		
		for(String componentId : iRichBolts.keySet()) {
			HashMap<Integer,IRichBolt> component = iRichBolts.get(componentId);
			for(Integer taskId : component.keySet()) {
				IRichBolt bolt = component.get(taskId);
				runComponentTask(bolt);
			}
		}
		for(String componentId : iRichSpouts.keySet()) {
			HashMap<Integer,IRichSpout> component = iRichSpouts.get(componentId);
			for(Integer taskId : component.keySet()) {
				IRichSpout spout = component.get(taskId);
				runComponentTask(spout);
			}
		}
		
		
	}
	
	public void runComponentTask(Runnable task) {
		if(!shouldTerminate) {
			componentExecutorService.execute(task);
		}
	}
	
	public void outputsScheduler(){
		for(int i=0;i<(Integer)conf.get(Config.DRAGON_NETWORK_THREADS);i++) {
			networkExecutorService.execute(new Runnable() {
				public void run(){
					while(!shouldTerminate) {
						try {
							Collector collector = outputsPending.take();
							//log.debug("network task pending "+outputsPending.size());
							synchronized(collector.lock) {
								//log.debug("peeking on queue");
								NetworkTask networkTask = (NetworkTask) collector.getQueue().peek();
								if(networkTask!=null) {
									//log.debug("processing network task");
									for(Integer taskId : networkTask.getTaskIds()) {
										Tuple tuple = networkTask.getTuple();
										String name = networkTask.getComponentId();
										if(iRichBolts.get(name).get(taskId).getInputCollector().getQueue().offer(tuple)){
											//log.debug("removing from queue");
											collector.getQueue().poll();
										} else {
											//log.debug("blocked");
											outputPending(collector);
										}
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
	
	public void outputPending(final Collector outputCollector) {
		try {
			outputsPending.put(outputCollector);
			//log.debug("outputPending pending "+outputsPending.size());
		} catch (InterruptedException e) {
			log.error("interruptep while adding output pending");
		}
	}
	
	public String getPersistanceDir(){
		return conf.get(Config.DRAGON_BASE_DIR)+"/"+conf.get(Config.DRAGON_PERSISTANCE_DIR);
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
	
}
