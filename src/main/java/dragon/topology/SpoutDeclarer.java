package dragon.topology;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import dragon.topology.base.Spout;

public class SpoutDeclarer extends Declarer {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2076109957656227105L;
	private static Log log = LogFactory.getLog(SpoutDeclarer.class);
	private Spout spout;
	
	public SpoutDeclarer(String name, int parallelismHint) {
		super(name, parallelismHint);
	}

	public SpoutDeclarer(String name, Spout spout, int parallelismHint) {
		super(name, parallelismHint);
		this.spout=spout;
	}
	
	public Spout getSpout() {
		return spout;
	}
	
}
