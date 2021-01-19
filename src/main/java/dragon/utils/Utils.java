package dragon.utils;

/**
 * @author aaron
 *
 */
public class Utils {
	/**
	 * @param ms
	 */
	public static void sleep(long ms){
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
