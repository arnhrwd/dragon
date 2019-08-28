package dragon.topology;

public class Declarer {
	private int numTasks;
	private String name;
	
	public Declarer(String name,int parallelismHint) {
		numTasks=parallelismHint;
		this.name=name;
	}
	
	public Declarer setNumTasks(int numTasks) {
		this.numTasks=numTasks;
		return this;
	}
}
