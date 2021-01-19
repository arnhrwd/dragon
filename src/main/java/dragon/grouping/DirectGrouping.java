package dragon.grouping;

import java.util.List;

import dragon.generated.GlobalStreamId;
import dragon.task.WorkerTopologyContext;

/**
 * @author aaron
 *
 */
public class DirectGrouping extends AbstractGrouping {
	private static final long serialVersionUID = -4021927336869082984L;

	/* (non-Javadoc)
	 * @see dragon.grouping.AbstractGrouping#chooseTasks(int, java.util.List)
	 */
	@Override
	public List<Integer> chooseTasks(int arg0, List<Object> values) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see dragon.grouping.AbstractGrouping#prepare(dragon.task.WorkerTopologyContext, dragon.generated.GlobalStreamId, java.util.List)
	 */
	@Override
	public void prepare(WorkerTopologyContext arg0, GlobalStreamId arg1, List<Integer> targetTasks) {
		// TODO Auto-generated method stub
		
	}

}
