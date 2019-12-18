package dragon;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.util.jar.JarFile;

/**
 * The currently accepted practice for programmatically adding
 * a JAR to the classpath. Hopefully Oracle doesn't kill this one :0)
 * @author aaron
 *
 */
public class Agent {
	
	private static Instrumentation inst;
	
	/**
	 * Add the JAR file to the class path.
	 * @param jarFile the JAR file to add to the class path
	 * @throws IOException thrown if there was a problem adding the JAR file
	 * to the class path
	 */
	public static void addToClassPath(File jarFile) throws IOException {
	  inst.appendToSystemClassLoaderSearch(new JarFile(jarFile));
	}

	/**
	 * Setup the instrumentation hook.
	 * @param agentArgs
	 * @param inst
	 * @throws IOException
	 */
    public static void premain(String agentArgs, Instrumentation inst) throws IOException {
    	Agent.inst=inst;
    }
}