package dragon;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
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
	public static final String DRAGON_PERSISTANCE_DIR="dragon.persistance.dir";
	public static final String DRAGON_NETWORK_THREADS="dragon.network.threads";
	public static final String DRAGON_OUTPUT_SCHEDULER_SLEEP="dragon.output.scheduler.sleep";
	public static final String DRAGON_COMPONENT_IDLE_TIME_MS="dragon.component.idle.time.ms";
	public static final String DRAGON_ROUTER_THREADS="dragon.router.threads";
	public static final String DRAGON_NETWORK_MAIN_NODE="dragon.network.main.node";
	public static final String DRAGON_NETWORK_SERVICE_PORT="dragon.network.service.port";
	
	int numWorkers=1;
	int maxTaskParallelism=1000;
	
	public Config() {
		super();
	}
	
	public Config(String file) throws IOException {
		super();
		// DEFAULTS
		put(DRAGON_OUTPUT_BUFFER_SIZE,1024);
		put(DRAGON_INPUT_BUFFER_SIZE,1024);
		put(DRAGON_BASE_DIR,"/tmp/dragon");
		put(DRAGON_PERSISTANCE_DIR,"persistance");
		put(DRAGON_NETWORK_THREADS,10);
		put(DRAGON_ROUTER_THREADS,10);
		put(DRAGON_OUTPUT_SCHEDULER_SLEEP,50);
		put(DRAGON_COMPONENT_IDLE_TIME_MS,50);
		put(DRAGON_NETWORK_MAIN_NODE,"localhost");
		put(DRAGON_NETWORK_SERVICE_PORT,4000);
		
		Properties props = new Properties();
        FileInputStream propStream = new FileInputStream(file);
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
}
