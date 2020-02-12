package dragon.topology;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.topology.base.Spout;

/**
 * Spout declarer doesn't have much to declare because spouts do not listen
 * to any other component. They simply emit tuples.
 * @author aaron
 * @see dragon.tuple.Tuple
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
