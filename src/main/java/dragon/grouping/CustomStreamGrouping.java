package dragon.grouping;

import java.util.List;

import dragon.generated.GlobalStreamId;
import dragon.task.WorkerTopologyContext;


public interface CustomStreamGrouping {
	public List<Integer> chooseTasks(int arg0, List<Object> values);
	public void prepare(WorkerTopologyContext arg0, GlobalStreamId arg1,
			List<Integer> targetTasks);
}
