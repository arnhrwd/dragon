package dragon;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.util.jar.JarFile;


public class Agent {
	
	private static Instrumentation inst;
	
	public static void addToClassPath(File jarFile) throws IOException {
	  inst.appendToSystemClassLoaderSearch(new JarFile(jarFile));
	}

    public static void premain(String agentArgs, Instrumentation inst) throws IOException {
    	Agent.inst=inst;
    }
}