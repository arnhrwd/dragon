package dragon;

import java.util.HashSet;

import dragon.tuple.Tuple;

public class NetworkTask {
	private Tuple tuple;
	private HashSet<Integer> taskIds;
	private String name;

	public NetworkTask(Tuple tuple,HashSet<Integer> taskIds,String name) {
		this.tuple=tuple;
		this.taskIds=taskIds;
		this.name=name;
	}
	
	public Tuple getTuple() {
		return tuple;
	}
	
	public HashSet<Integer> getTaskIds(){
		return taskIds;
	}
	
	public String getName() {
		return name;
	}
}
