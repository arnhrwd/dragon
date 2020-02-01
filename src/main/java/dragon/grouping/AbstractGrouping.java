package dragon.grouping;

import java.io.Serializable;
import java.util.List;

import dragon.generated.GlobalStreamId;
import dragon.task.WorkerTopologyContext;

/**
 * @author aaron
 *
 */
public  abstract class AbstractGrouping implements Serializable {
	private static final long serialVersionUID = -770441274639731781L;
	
	/**
	 * Specify which tasks a tuple should go.
	 * @param arg0
	 * @param values
	 * @return 
	 */
	public abstract List<Integer> chooseTasks(int arg0, List<Object> values);
	
	/**
	 * @param arg0
	 * @param arg1
	 * @param targetTasks
	 */
	public abstract void prepare(WorkerTopologyContext arg0, GlobalStreamId arg1, List<Integer> targetTasks);
}
