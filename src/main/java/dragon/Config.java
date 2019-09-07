package dragon;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Config extends HashMap<String, Object>{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5933157870455074368L;
	private static Log log = LogFactory.getLog(Config.class);

	public static final String TOPOLOGY_WORKERS="topology.workers";
	public static final String TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS="topology.trident.batch.emit.interval.millis";
	public static final String TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS="topology.enable.message.timeouts";
	public static final String TOPOLOGY_MAX_SPOUT_PENDING="topology.max.spout.pending";
	public static final String TOPOLOGY_MESSAGE_TIMEOUT_SECS="topology.message.timeout.secs";
	public static final String TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE="topology.executor.receive.buffer.size";
	public static final String TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE="topology.executor.send.buffer.size";
	public static final String TOPOLOGY_TICK_TUPLE_FREQ_SECS="topology.tick.tuple.freq.secs";
	
	public static final String DRAGON_OUTPUT_BUFFER_SIZE="dragon.output.buffer.size";
	public static final String DRAGON_INPUT_BUFFER_SIZE="dragon.input.buffer.size";
	public static final String DRAGON_BASE_DIR="dragon.base.dir";
	public static final String DRAGON_JAR_DIR="dragon.jar.dir";
	public static final String DRAGON_PERSISTANCE_DIR="dragon.persistance.dir";
	public static final String DRAGON_LOCALCLUSTER_THREADS="dragon.localcluster.threads";
	public static final String DRAGON_ROUTER_INPUT_THREADS="dragon.router.input.threads";
	public static final String DRAGON_ROUTER_OUTPUT_THREADS="dragon.router.output.threads";
	public static final String DRAGON_ROUTER_INPUT_BUFFER_SIZE="dragon.router.input.buffer.size";
	public static final String DRAGON_ROUTER_OUTPUT_BUFFER_SIZE="dragon.router.output.buffer.size";
	public static final String DRAGON_NETWORK_REMOTE_HOST="dragon.network.remote.host";
	public static final String DRAGON_NETWORK_LOCAL_HOST="dragon.network.local.host";
	public static final String DRAGON_NETWORK_REMOTE_SERVICE_PORT="dragon.network.remote.service.port";
	public static final String DRAGON_NETWORK_LOCAL_SERVICE_PORT="dragon.network.local.service.port";
	public static final String DRAGON_NETWORK_REMOTE_NODE_PORT="dragon.network.remote.node.port";
	public static final String DRAGON_NETWORK_LOCAL_NODE_PORT="dragon.network.local.node.port";
	
	int numWorkers=1;
	int maxTaskParallelism=1000;
	
	public Config() {
		super();
		defaults();
	}
	
	public Config(String file) throws IOException {
		super();
		defaults();
		
		
		Properties props = new Properties();
		FileInputStream propStream=null;
		try {
			log.debug("looking for "+file+" in working directory");
			propStream = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			
		}
		if(propStream==null) {
			try {
				log.debug("looking for "+file+" in ../conf");
				propStream = new FileInputStream("../conf/"+file);
			} catch (FileNotFoundException e) {
				
			}
		}
		if(propStream==null) {
			try {
				log.debug("looking for "+file+" in /etc/dragon");
				propStream = new FileInputStream("/etc/dragon/"+file);
			} catch (FileNotFoundException e) {
				
			}
		}
		if(propStream==null) {
			try {
				String home = System.getenv("HOME");
				log.debug("looking for "+file+" in "+home+"/.dragon");
				propStream = new FileInputStream(home+"/.dragon/"+file);
			} catch (FileNotFoundException e) {
				
			}
		}
		if(propStream==null) {
			log.warn("cannot find "+file+" - using defaults");
			return;
		}
		props.load(propStream);
        for(Object prop : props.keySet()) {
        	String propName = (String) prop;
        	if(containsKey(propName)) {
        		if(get(propName) instanceof String) {
        			put(propName,props.getProperty(propName));
        		} else if(get(propName) instanceof Integer) {
        			put(propName,Integer.parseInt(props.getProperty(propName)));
        		}
        	} else {
        		log.error(propName+" is unknown, ignoring");
        	}
        }
	}
	
	public void defaults() {
		put(DRAGON_OUTPUT_BUFFER_SIZE,1024);
		put(DRAGON_INPUT_BUFFER_SIZE,1024);
		put(DRAGON_BASE_DIR,"/tmp/dragon");
		put(DRAGON_PERSISTANCE_DIR,"persistance");
		put(DRAGON_JAR_DIR,"jars");
		put(DRAGON_LOCALCLUSTER_THREADS,10);
		put(DRAGON_ROUTER_INPUT_THREADS,10);
		put(DRAGON_ROUTER_OUTPUT_THREADS,10);
		put(DRAGON_ROUTER_INPUT_BUFFER_SIZE,1024);
		put(DRAGON_ROUTER_OUTPUT_BUFFER_SIZE,1024);
		put(DRAGON_NETWORK_REMOTE_HOST,"");
		put(DRAGON_NETWORK_LOCAL_HOST,"localhost");
		put(DRAGON_NETWORK_REMOTE_SERVICE_PORT,4000);
		put(DRAGON_NETWORK_LOCAL_SERVICE_PORT,4000);
		put(DRAGON_NETWORK_REMOTE_NODE_PORT,4001);
		put(DRAGON_NETWORK_LOCAL_NODE_PORT,4001);
	}

	public void setNumWorkers(int numWorkers) {
		this.numWorkers=numWorkers;
	}
	
	public int getNumberWorkers() {
		return this.numWorkers;
	}
	
	public void setMaxTaskParallelism(int maxTaskParallelism) {
		this.maxTaskParallelism=maxTaskParallelism;
	}
	
	public int getMaxTaskParallelism() {
		return this.maxTaskParallelism;
	}
	
	public String getJarDir() {
		return get(Config.DRAGON_BASE_DIR)+"/"+get(Config.DRAGON_JAR_DIR);
	}
}
