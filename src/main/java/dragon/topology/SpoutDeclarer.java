package dragon.topology;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;


import dragon.topology.base.Spout;

public class SpoutDeclarer extends Declarer {
	private static final long serialVersionUID = 2076109957656227105L;
	@SuppressWarnings("unused")
	private static Logger log = LogManager.getLogger(SpoutDeclarer.class);
	private Spout spout;
	
	public SpoutDeclarer(int parallelismHint) {
		super(parallelismHint);
	}

	public SpoutDeclarer(Spout spout, int parallelismHint) {
		super(parallelismHint);
		this.spout=spout;
	}
	
	public Spout getSpout() {
		return spout;
	}
	
}
