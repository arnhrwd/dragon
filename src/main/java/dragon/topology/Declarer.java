package dragon.topology;

import java.io.Serializable;

public class Declarer implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4580590809365867826L;
	private int numTasks;
	private int parallelismHint;
	private String componentId;
	
	public Declarer(String componentId,int parallelismHint) {
		this.parallelismHint=parallelismHint;
		numTasks=parallelismHint;
		this.componentId=componentId;
	}
	
	public Declarer setNumTasks(int numTasks) {
		this.numTasks=numTasks;
		return this;
	}
	
	public int getNumTasks() {
		return numTasks;
	}
	
	public int getParallelismHint() {
		return parallelismHint;
	}
}
