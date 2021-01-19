package dragon;

/**
 * Exception for indicating that a clonable is required.
 * @author aaron
 *
 */
public class DragonRequiresClonableException extends Exception {
	private static final long serialVersionUID = 7090376269379039359L;
	public DragonRequiresClonableException(String msg) {
		super(msg);
	}
}
