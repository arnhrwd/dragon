package dragon.grouping;

import java.util.List;

import dragon.generated.GlobalStreamId;
import dragon.task.WorkerTopologyContext;
import dragon.tuple.Fields;

/**
 * @author aaron
 *
 */
public class FieldGrouping  extends AbstractGrouping  {
	private static final long serialVersionUID = -7755313939232587197L;
	
	/**
	 * 
	 */
	private List<Integer> targetTasks;

	/**
	 * @param fields
	 */
	public FieldGrouping(Fields fields) {
		
	}

	/* (non-Javadoc)
	 * @see dragon.grouping.AbstractGrouping#chooseTasks(int, java.util.List)
	 */
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

	/* (non-Javadoc)
	 * @see dragon.grouping.AbstractGrouping#prepare(dragon.task.WorkerTopologyContext, dragon.generated.GlobalStreamId, java.util.List)
	 */
	@Override
	public void prepare(WorkerTopologyContext arg0, GlobalStreamId arg1, List<Integer> targetTasks) {
		this.targetTasks=targetTasks;
	}

}
