package dragon.topology;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;


import dragon.topology.base.Spout;

/**
 * @author aaron
 *
 */
public class SpoutDeclarer extends Declarer {
	private static final long serialVersionUID = 2076109957656227105L;
	@SuppressWarnings("unused")
	private static Logger log = LogManager.getLogger(SpoutDeclarer.class);
	
	/**
	 * 
	 */
	private Spout spout;
	
	/**
	 * @param parallelismHint
	 */
	public SpoutDeclarer(int parallelismHint) {
		super(parallelismHint);
	}

	/**
	 * @param spout
	 * @param parallelismHint
	 */
	public SpoutDeclarer(Spout spout, int parallelismHint) {
		super(parallelismHint);
		this.spout=spout;
	}
	
	/**
	 * @return
	 */
	public Spout getSpout() {
		return spout;
	}
	
}
