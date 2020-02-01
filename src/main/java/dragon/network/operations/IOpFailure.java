package dragon.network.operations;

/**
 * @author aaron
 *
 */
public interface IOpFailure {
	/**
	 * @param op
	 * @param error
	 */
	public void fail(Op op,String error);
}
