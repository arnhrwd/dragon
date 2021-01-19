package dragon.topology;

import java.io.Serializable;

/**
 * @author aaron
 *
 */
public class Declarer implements Serializable {
	private static final long serialVersionUID = 4580590809365867826L;
	
	/**
	 * 
	 */
	private int numTasks;
	
	/**
	 * 
	 */
	private int parallelismHint;
	
	/**
	 * @param parallelismHint
	 */
	public Declarer(int parallelismHint) {
		this.parallelismHint=parallelismHint;
		numTasks=parallelismHint;
	}
	
	/**
	 * @param numTasks
	 * @return
	 */
	public Declarer setNumTasks(int numTasks) {
		this.numTasks=numTasks;
		return this;
	}
	
	/**
	 * @return
	 */
	public int getNumTasks() {
		return numTasks;
	}
	
	/**
	 * @return
	 */
	public int getParallelismHint() {
		return parallelismHint;
	}
}
