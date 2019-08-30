package dragon.grouping;

import java.util.List;

import dragon.generated.GlobalStreamId;
import dragon.task.WorkerTopologyContext;

public class AllGrouping extends AbstractGrouping {

	private List<Integer> targetTasks;

	@Override
	public List<Integer> chooseTasks(int arg0, List<Object> values) {
		return targetTasks;
	}

	@Override
	public void prepare(WorkerTopologyContext arg0, GlobalStreamId arg1, List<Integer> targetTasks) {
		this.targetTasks=targetTasks;
	}

}
