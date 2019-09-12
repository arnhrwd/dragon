package dragon.grouping;

import java.io.Serializable;
import java.util.List;

import dragon.generated.GlobalStreamId;
import dragon.task.WorkerTopologyContext;

public  abstract class AbstractGrouping implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -770441274639731781L;

	public abstract List<Integer> chooseTasks(int arg0, List<Object> values);
	

	public abstract void prepare(WorkerTopologyContext arg0, GlobalStreamId arg1, List<Integer> targetTasks);
	

}
