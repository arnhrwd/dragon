package dragon.utils;

/***
 * A class to be used for loading classes in runtime.
 * @author gayashan
 */
public class ReflectionUtils {
    /**
     * 
     */
    private static ReflectionUtils _instance = new ReflectionUtils();

    /**
     * @param klass
     * @return
     */
    public static <T> T newInstance(String klass) {
        try {
            return newInstance((Class<T>) Class.forName(klass));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param klass
     * @return
     */
    public static <T> T newInstance(Class<T> klass) {
    	return _instance.newInstanceImpl(klass);
    }

    /**
     * @param klass
     * @return
     */
    public <T> T newInstanceImpl(Class<T> klass) {
        try {
            return klass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
