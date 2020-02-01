package dragon.metrics;


import java.util.HashMap;

/**
 * For storing metrics samples on components.
 * @author aaron
 *
 */
public class ComponentMetricMap extends HashMap<String,TaskMetricMap>{
	private static final long serialVersionUID = 8719152068849752492L;

	/**
	 * 
	 */
	private int sampleHistory;
	
	/**
	 * @param sampleHistory
	 */
	public ComponentMetricMap(int sampleHistory){
		this.sampleHistory = sampleHistory;
	}
	
	/**
	 * @param componentId
	 * @param taskId
	 * @param sample
	 */
	public void put(String componentId, Integer taskId, Sample sample) {
		if(!containsKey(componentId)){
			put(componentId,new TaskMetricMap(sampleHistory));
		}
		get(componentId).put(taskId, sample);
	}
	
	/* (non-Javadoc)
	 * @see java.util.AbstractMap#toString()
	 */
	public String toString(){
		String out = "";
		for(String componentId : keySet()){
			out += componentId +"\n";
			out += get(componentId).toString();
		}
		return out;
	}

}
