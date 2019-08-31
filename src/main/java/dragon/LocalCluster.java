package dragon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.grouping.CustomStreamGrouping;
import dragon.spout.SpoutOutputCollector;
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
		
		private HashSet<Collector> outputsPending;
		private Thread outputsSchedulerThread;
		
		public LocalCluster() {
			
		}
	
		void submitTopology(String topologyName, Config conf, DragonTopology dragonTopology) {
			this.topologyName=topologyName;
			this.conf=conf;
			this.dragonTopology=dragonTopology;
			outputsPending = new HashSet<Collector>();
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
						OutputCollector collector = new OutputCollector(this,bolt);
						
						bolt.prepare(conf, context, collector);
						
					} catch (CloneNotSupportedException e) {
						log.error("could not clone object: "+e.toString());
					}
				}
			}
			
			// prepare groupings
			for(String fromComponentId : dragonTopology.spoutMap.keySet()) {
				HashMap<String,HashMap<String,HashSet<CustomStreamGrouping>>> 
					fromComponent = dragonTopology.getFromComponent(fromComponentId);
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
			componentExecutorService.execute(task);
		}
		
		public void outputsScheduler(){
			outputsSchedulerThread = new Thread(){
				public void run(){
					ArrayList<Collector> oc;
					while(!isInterrupted()){
						synchronized(outputsPending){
							oc=new ArrayList<Collector>(outputsPending);
						}
						if(oc.size()>0){
							for(Collector outputCollector : oc){
								scheduleNetworkTask(outputCollector);
							}
						} else {
							try {
								sleep((Integer)conf.get(Config.DRAGON_OUTPUT_SCHEDULER_SLEEP));
							} catch (InterruptedException e) {
								log.info("interrupted");
							}
						}
					}
				}
			};
			outputsSchedulerThread.run();
		}
		
		public void scheduleNetworkTask(final Collector outputCollector){
			networkExecutorService.execute(new Runnable(){
				public void run() {
					boolean reschedule=false;
					synchronized(outputsPending){
						outputsPending.remove(outputCollector);
					}
					
					NetworkTask networkTask = outputCollector.getQueue().peek();
					while(networkTask!=null){
						for(Integer taskId : networkTask.getTaskIds()) {
							Tuple tuple = networkTask.getTuple();
							String name = networkTask.getName();
							if(iRichBolts.get(name).get(taskId).getInputCollector().getQueue().offer(tuple)){
								outputCollector.getQueue().poll();
								networkTask = outputCollector.getQueue().peek();
							} else {
								reschedule=true;
								break;
							}
						}
					}
					if(reschedule){
						synchronized(outputsPending){
							outputsPending.add(outputCollector);
						}
					}
				}
			});
		}
		
		public void outputPending(final Collector outputCollector) {
			synchronized(outputsPending){
				outputsPending.add(outputCollector);
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

		
	
}
