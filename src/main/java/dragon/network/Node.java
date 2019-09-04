package dragon.network;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.LocalCluster;
import dragon.network.messages.service.RunTopology;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TopologyExists;
import dragon.network.messages.service.TopologySubmitted;

public class Node {
	private static Log log = LogFactory.getLog(Node.class);
	private IComms comms;
	private ExecutorService networkExecutorService;
	private HashMap<String,LocalCluster> localClusters;
	private Thread serviceThread;
	private boolean shouldTerminate=false;
	public Node() {
		comms = new TcpComms();
		log.debug("starting comms");
		comms.open(false);
		serviceThread=new Thread(){
			public void run(){
				while(!shouldTerminate){
					ServiceMessage command = comms.receiveServiceMessage();
					switch(command.getType()){
					case RUN_TOPOLOGY:
						RunTopology scommand = (RunTopology) command;
						if(localClusters.containsKey(scommand.topologyName)){
							comms.sendServiceMessage(new TopologyExists(scommand.topologyName));
						} else {
							LocalCluster cluster=new LocalCluster();
							cluster.submitTopology(scommand.topologyName, scommand.conf, scommand.dragonTopology);
							localClusters.put(scommand.topologyName, cluster);
							comms.sendServiceMessage(new TopologySubmitted(scommand.topologyName));
						}
						break;
					default:
					}
				}
			}
		};
		log.debug("starting service thread");
		serviceThread.start();
		
	}
	
	
}
