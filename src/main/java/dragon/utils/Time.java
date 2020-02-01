package dragon.utils;

import java.util.Date;

/**
 * @author aaron
 *
 */
public class Time {
	/**
	 * @return
	 */
	public static long currentTimeMillis(){
		return new Date().getTime();
	}
}
