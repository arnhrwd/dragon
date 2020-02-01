package dragon.metrics;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Keeps samples for a given task, upto sampleHistory of them.
 * @author aaron
 *
 */
public class TaskMetricMap extends HashMap<Integer,ArrayList<Sample>>{
	private static final long serialVersionUID = 1514387987198642265L;
	
	/**
	 * 
	 */
	private int sampleHistory;
	
	/**
	 * @param sampleHistory
	 */
	public TaskMetricMap(int sampleHistory){
		this.sampleHistory=sampleHistory;
	}
	
	/**
	 * @param taskId
	 * @param sample
	 */
	public void put(Integer taskId, Sample sample) {
		if(!containsKey(taskId)){
			put(taskId,new ArrayList<Sample>());
		}
		get(taskId).add(sample);
		if(get(taskId).size()>sampleHistory){
			get(taskId).remove(0);
		}
		
	}
	
	/* (non-Javadoc)
	 * @see java.util.AbstractMap#toString()
	 */
	public String toString(){
		String out = "";
		for(Integer taskId : keySet()){
			out+=taskId+"\n";
			for(Sample sample : get(taskId)){
				out+=sample.toString();
			}
		}
		return out;
	}

}
