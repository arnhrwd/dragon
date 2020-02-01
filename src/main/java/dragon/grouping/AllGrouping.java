package dragon.grouping;

import java.util.List;

import dragon.generated.GlobalStreamId;
import dragon.task.WorkerTopologyContext;

/**
 * @author aaron
 *
 */
public class AllGrouping extends AbstractGrouping {
	private static final long serialVersionUID = -1445957562325407092L;
	
	/**
	 * 
	 */
	private List<Integer> targetTasks;

	/* (non-Javadoc)
	 * @see dragon.grouping.AbstractGrouping#chooseTasks(int, java.util.List)
	 */
	@Override
	public List<Integer> chooseTasks(int arg0, List<Object> values) {
		return targetTasks;
	}

	/* (non-Javadoc)
	 * @see dragon.grouping.AbstractGrouping#prepare(dragon.task.WorkerTopologyContext, dragon.generated.GlobalStreamId, java.util.List)
	 */
	@Override
	public void prepare(WorkerTopologyContext arg0, GlobalStreamId arg1, List<Integer> targetTasks) {
		this.targetTasks=targetTasks;
	}

}
