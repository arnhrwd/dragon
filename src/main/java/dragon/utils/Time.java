package dragon.utils;

import java.util.Date;

public class Time {
	public static long currentTimeMillis(){
		return new Date().getTime();
	}
}
