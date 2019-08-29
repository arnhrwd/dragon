package dragon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.task.OutputCollector;
import dragon.topology.DragonTopology;
import dragon.topology.base.IRichBolt;
import dragon.tuple.Tuple;


public class LocalCluster {
		private Log log = LogFactory.getLog(LocalCluster.class);
		private HashMap<String,HashMap<Integer,IRichBolt>> iRichBolts;
		private ExecutorService componentExecutorService;
		private ExecutorService networkExecutorService;
		
		private String topologyName;
		private Config conf;
		private DragonTopology dragonTopology;
		
		private HashSet<OutputCollector> outputsPending;
		private Thread outputsSchedulerThread;
		
		public LocalCluster() {
			
		}
	
		void submitTopology(String topologyName, Config conf, DragonTopology dragonTopology) {
			this.topologyName=topologyName;
			this.conf=conf;
			this.dragonTopology=dragonTopology;
			outputsPending = new HashSet<OutputCollector>();
			networkExecutorService = Executors.newFixedThreadPool((Integer)conf.get(Config.DRAGON_NETWORK_THREADS));
		}
		
		public void runTask(Runnable task) {
			
		}
		
		public void outputsScheduler(){
			outputsSchedulerThread = new Thread(){
				public void run(){
					ArrayList<OutputCollector> oc;
					while(!isInterrupted()){
						synchronized(outputsPending){
							oc=new ArrayList<OutputCollector>(outputsPending);
						}
						if(oc.size()>0){
							for(OutputCollector outputCollector : oc){
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
		
		public void scheduleNetworkTask(final OutputCollector outputCollector){
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
		
		public void outputPending(final OutputCollector outputCollector) {
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
	
}
