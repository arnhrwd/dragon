package dragon.network;

/**
 * For raising exceptions concerning operations on topologies,
 * usually when the named topology does not exist.
 * @author aaron
 *
 */
public class DragonTopologyException extends Exception {
	private static final long serialVersionUID = -928215567976343494L;
	
	/**
	 * @param message
	 */
	public DragonTopologyException(String message) {
		super(message);
	}
}
