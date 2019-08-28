package dragon;

import java.util.HashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Config {
	private Log log = LogFactory.getLog(Config.class);
	HashMap<String,Object> conf;
	int numWorkers=1;
	int maxTaskParallelism=1000;
	public final String TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS="TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS";
	public final String TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS="TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS";
	public final String TOPOLOGY_MAX_SPOUT_PENDING="TOPOLOGY_MAX_SPOUT_PENDING";
	public final String TOPOLOGY_MESSAGE_TIMEOUT_SECS="TOPOLOGY_MESSAGE_TIMEOUT_SECS";
	
	public Config() {
		conf = new HashMap<String,Object>();
	}
	
	public Object put(String key, Object value) {
		return conf.put(key, value);
	}
	
	public void setNumberWorkers(int numWorkers) {
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
