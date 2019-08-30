package dragon.grouping;

import java.util.List;

import dragon.generated.GlobalStreamId;
import dragon.task.WorkerTopologyContext;
import dragon.tuple.Fields;

public class FieldGrouping  extends AbstractGrouping  {
	private List<Integer> targetTasks;
	Fields fields;

	public FieldGrouping(Fields fields) {
		this.fields=fields;
	}

	@Override
	public List<Integer> chooseTasks(int arg0, List<Object> values) {
		int hash=0;
		for(Object obj : values) {
			hash=hash ^ obj.hashCode();
		}
		if(hash<0)hash=-hash;
		hash=hash%targetTasks.size();
		return(targetTasks.subList(hash,hash+1));
	}

	@Override
	public void prepare(WorkerTopologyContext arg0, GlobalStreamId arg1, List<Integer> targetTasks) {
		this.targetTasks=targetTasks;
	}

}
