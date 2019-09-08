package dragon.grouping;

import java.io.Serializable;
import java.util.List;

import dragon.generated.GlobalStreamId;
import dragon.task.WorkerTopologyContext;

public  class AbstractGrouping implements CustomStreamGrouping, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -770441274639731781L;

	public List<Integer> chooseTasks(int arg0, List<Object> values) {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(WorkerTopologyContext arg0, GlobalStreamId arg1, List<Integer> targetTasks) {
		// TODO Auto-generated method stub
		
	}
	

}
