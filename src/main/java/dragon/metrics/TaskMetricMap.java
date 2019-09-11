package dragon.metrics;

import java.util.ArrayList;
import java.util.HashMap;

public class TaskMetricMap extends HashMap<Integer,ArrayList<Sample>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1514387987198642265L;
	private int sampleHistory;
	public TaskMetricMap(int sampleHistory){
		this.sampleHistory=sampleHistory;
	}
	
	public void put(Integer taskId, Sample sample) {
		if(!containsKey(taskId)){
			put(taskId,new ArrayList<Sample>());
		}
		get(taskId).add(sample);
		if(get(taskId).size()>sampleHistory){
			get(taskId).remove(0);
		}
		
	}
	
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
