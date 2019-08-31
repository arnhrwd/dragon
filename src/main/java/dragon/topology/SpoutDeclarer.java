package dragon.topology;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.topology.base.IRichSpout;

public class SpoutDeclarer extends Declarer {
	private Log log = LogFactory.getLog(SpoutDeclarer.class);
	private IRichSpout spout;
	
	public SpoutDeclarer(String name, int parallelismHint) {
		super(name, parallelismHint);
	}

	public SpoutDeclarer(String name, IRichSpout spout, int parallelismHint) {
		super(name, parallelismHint);
		this.spout=spout;
	}
	
	public IRichSpout getSpout() {
		return spout;
	}
	
}
