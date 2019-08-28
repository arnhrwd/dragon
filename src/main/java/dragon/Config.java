package dragon;

import java.util.HashMap;

public class Config {
	HashMap<String,Object> conf;
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
		
	}
	
	public void setMaxTaskParallelism(int maxTaskParallelism) {
		
	}
}
