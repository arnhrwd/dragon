package dragon;

import java.util.HashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Config extends HashMap<String, Object>{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5933157870455074368L;


	private static Log log = LogFactory.getLog(Config.class);

	
	int numWorkers=1;
	int maxTaskParallelism=1000;
	
	public static final String TOPOLOGY_WORKERS="TOPOLOGY_WORKERS";
	public static final String TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS="TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS";
	public static final String TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS="TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS";
	public static final String TOPOLOGY_MAX_SPOUT_PENDING="TOPOLOGY_MAX_SPOUT_PENDING";
	public static final String TOPOLOGY_MESSAGE_TIMEOUT_SECS="TOPOLOGY_MESSAGE_TIMEOUT_SECS";
	public static final String TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE="TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE";
	public static final String TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE="TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE";
	public static final String TOPOLOGY_TICK_TUPLE_FREQ_SECS="TOPOLOGY_TICK_TUPLE_FREQ_SECS";
	
	public static final String DRAGON_OUTPUT_BUFFER_SIZE="DRAGON_OUTPUT_BUFFER_SIZE";
	public static final String DRAGON_INPUT_BUFFER_SIZE="DRAGON_INPUT_BUFFER_SIZE";
	public static final String DRAGON_BASE_DIR="DRAGON_BASE_DIR";
	public static final String DRAGON_PERSISTANCE_DIR="DRAGON_PERSISTANCE_DIR";
	public static final String DRAGON_NETWORK_THREADS="DRAGON_NETWORK_THREADS";
	public static final String DRAGON_OUTPUT_SCHEDULER_SLEEP="DRAGON_OUTPUT_SCHEDULER_SLEEP";
	public static final String DRAGON_COMPONENT_IDLE_TIME_MS="DRAGON_COMPONENT_IDLE_TIME_MS";
	
	public Config() {
		super();
		put(DRAGON_OUTPUT_BUFFER_SIZE,1024);
		put(DRAGON_INPUT_BUFFER_SIZE,1024);
		put(DRAGON_BASE_DIR,"/tmp/dragon");
		put(DRAGON_PERSISTANCE_DIR,"persistance");
		put(DRAGON_NETWORK_THREADS,10);
		put(DRAGON_OUTPUT_SCHEDULER_SLEEP,50);
		put(DRAGON_COMPONENT_IDLE_TIME_MS,50);
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
