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
	 * @param taskIndex
	 * @param sample
	 */
	public void put(Integer taskIndex, Sample sample) {
		if(!containsKey(taskIndex)){
			put(taskIndex,new ArrayList<Sample>());
		}
		get(taskIndex).add(sample);
		if(get(taskIndex).size()>sampleHistory){
			get(taskIndex).remove(0);
		}
		
	}
	
	/* (non-Javadoc)
	 * @see java.util.AbstractMap#toString()
	 */
	public String toString(){
		String out = "";
		for(Integer taskIndex : keySet()){
			out+=taskIndex+"\n";
			for(Sample sample : get(taskIndex)){
				out+=sample.toString();
			}
		}
		return out;
	}

}
