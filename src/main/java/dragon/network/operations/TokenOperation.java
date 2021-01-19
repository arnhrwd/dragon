package dragon.network.operations;

/**
 * @author aaron
 *
 */
public class TokenOperation extends Op {
	private static final long serialVersionUID = 490804555833692713L;

	/**
	 * @param success
	 * @param failure
	 */
	public TokenOperation(IOpSuccess success, IOpFailure failure) {
		super(success, failure);
	}

}
