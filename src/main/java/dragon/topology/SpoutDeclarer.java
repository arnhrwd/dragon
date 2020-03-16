package dragon.topology;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.topology.base.Spout;

/**
 * Spout declarer doesn't have much to declare because spouts do not listen
 * to any other components. They simply emit tuples.
 * @author aaron
 * @see dragon.tuple.Tuple
 *
 */
public class SpoutDeclarer extends Declarer {
	private static final long serialVersionUID = 2076109957656227105L;
	@SuppressWarnings("unused")
	private static Logger log = LogManager.getLogger(SpoutDeclarer.class);
	
	/**
	 * The prototype spout. Anything that is set in the constructor of this
	 * spout will be cloned to all spout instances. Runtime specific state
	 * can not be set within the constructor and is usually set within the open
	 * method.
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
