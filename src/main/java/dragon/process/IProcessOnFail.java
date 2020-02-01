package dragon.process;

/**
 * @author aaron
 *
 */
public interface IProcessOnFail {
	/**
	 * @param pb
	 */
	public void fail(ProcessBuilder pb);
}
