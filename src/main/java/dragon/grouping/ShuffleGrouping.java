package dragon.grouping;

import java.util.List;

import dragon.generated.GlobalStreamId;
import dragon.task.WorkerTopologyContext;

public class ShuffleGrouping extends AbstractGrouping  {
	private static final long serialVersionUID = 6002992363152902945L;
	private List<Integer> targetTasks;
	private int index;
	
	@Override
	public List<Integer> chooseTasks(int arg0, List<Object> values) {
		index=(index+1)%targetTasks.size();
		return(targetTasks.subList(index,index+1));
	}

	@Override
	public void prepare(WorkerTopologyContext arg0, GlobalStreamId arg1, List<Integer> targetTasks) {
		index=0;
		this.targetTasks=targetTasks;
	}

}
