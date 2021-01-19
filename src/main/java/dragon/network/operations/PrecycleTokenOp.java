package dragon.network.operations;

/**
 * @author aaron
 *
 */
public class PrecycleTokenOp extends TokenOperation {
	private static final long serialVersionUID = 8308879753027304391L;

	/**
	 * @param success
	 * @param failure
	 */
	public PrecycleTokenOp(IOpSuccess success, IOpFailure failure) {
		super(success, failure);
	}

}
