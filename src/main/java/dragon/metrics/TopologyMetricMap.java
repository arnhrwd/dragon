package dragon.metrics;


import java.util.HashMap;

public class TopologyMetricMap extends HashMap<String,ComponentMetricMap> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -671454144476472538L;

	private int sampleHistory;
	
	public TopologyMetricMap(int sampleHistory){
		this.sampleHistory=sampleHistory;
	}
	
	public void put(String topologyId, String componentId, Integer taskId, Sample sample) {
		if(!containsKey(topologyId)){
			put(topologyId,new ComponentMetricMap(sampleHistory));
		}
		get(topologyId).put(componentId, taskId, sample);
		
	}

}
