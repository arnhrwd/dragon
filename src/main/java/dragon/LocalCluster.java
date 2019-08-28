package dragon;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import dragon.topology.DragonTopology;
import dragon.topology.base.IRichBolt;
import dragon.tuple.Tuple;

public class LocalCluster {
		private Log log = LogFactory.getLog(LocalCluster.class);
		private HashMap<String,HashMap<Integer,IRichBolt>> iRichBolts;
		private ExecutorService componentExecutorService;
		private ExecutorService networkExecutorService;
		
		public LocalCluster() {
			
		}
	
		void submitTopology(String topologyName, Config conf, DragonTopology dragonTopology) {
			
		}
		
		public void runTask(Runnable task) {
			
		}
		
		public void outputPending(final LinkedBlockingQueue<NetworkTask> outputQueue) {
			runTask(new Runnable() {
				
				public void run() {
					try {
						NetworkTask networkTask = outputQueue.take();
						
						for(Integer taskId : networkTask.getTaskIds()) {
							Tuple tuple = networkTask.getTuple();
							String name = networkTask.getName();
							iRichBolts.get(name).get(taskId).getInputCollector().receive(tuple);
						}
					} catch (InterruptedException e) {
						log.error("interrupted while taking from outputQueue or putting on receive queue");
					}
				}
				
			});
		}
	
}
