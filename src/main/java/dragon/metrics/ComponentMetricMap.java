package dragon.metrics;


import java.util.HashMap;

public class ComponentMetricMap extends HashMap<String,TaskMetricMap>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8719152068849752492L;

	private int sampleHistory;
	
	public ComponentMetricMap(int sampleHistory){
		this.sampleHistory = sampleHistory;
	}
	
	public void put(String componentId, Integer taskId, Sample sample) {
		if(!containsKey(componentId)){
			put(componentId,new TaskMetricMap(sampleHistory));
		}
		get(componentId).put(taskId, sample);
	}
	
	public String toString(){
		String out = "";
		for(String componentId : keySet()){
			out += componentId +"\n";
			out += get(componentId).toString();
		}
		return out;
	}

}
