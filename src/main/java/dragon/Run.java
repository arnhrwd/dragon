package dragon;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Run {
	private static Log log = LogFactory.getLog(Run.class);
	
	// Returns an arraylist of class names in a JarInputStream
	private static ArrayList<String> getClassNamesFromJar(JarInputStream jarFile) throws Exception {
	    ArrayList<String> classNames = new ArrayList<String>();
	    try {
	        //JarInputStream jarFile = new JarInputStream(jarFileStream);
	        JarEntry jar;
	 
	        //Iterate through the contents of the jar file
	        while (true) {
	            jar = jarFile.getNextJarEntry();
	            if (jar == null) {
	                break;
	            }
	            //Pick file that has the extension of .class
	            if ((jar.getName().endsWith(".class"))) {
	                String className = jar.getName().replaceAll("/", "\\.");
	                String myClass = className.substring(0, className.lastIndexOf('.'));
	                classNames.add(myClass);
	            }
	        }
	    } catch (Exception e) {
	        throw new Exception("Error while getting class names from jar", e);
	    }
	    return classNames;
	}
	 
	// Returns an arraylist of class names in a JarInputStream
	// Calls the above function by converting the jar path to a stream
	private static  ArrayList<String> getClassNamesFromJar(String jarPath) throws Exception {
	    return getClassNamesFromJar(new JarInputStream(new FileInputStream(jarPath)));
	}
	
	// get an arraylist of all the loaded classes in a jar file
	@SuppressWarnings("rawtypes")
	private static ArrayList<Class> loadJarFile(String filePath) throws Exception {
	 
	    ArrayList<Class> availableClasses = new ArrayList<Class>();
	     
	    ArrayList<String> classNames = getClassNamesFromJar(filePath);
	    File f = new File(filePath);
	 
	    URLClassLoader classLoader = new URLClassLoader(new URL[]{f.toURI().toURL()});
	    for (String className : classNames) {
	        try {
	            Class cc = classLoader.loadClass(className);
	            availableClasses.add(cc);
	        } catch (ClassNotFoundException e) {
	            log.error("Class " + className + " was not found! "+e.toString());
	        }
	    }
	    classLoader.close();
	    return availableClasses;
	}
	
	@SuppressWarnings("rawtypes")
	private static Class loadJarFileClass(String filePath, String className) throws ClassNotFoundException, IOException  {
		File f = new File(filePath);
		URLClassLoader classLoader = new URLClassLoader(new URL[]{f.toURI().toURL()});
		Class c = classLoader.loadClass(className);
		classLoader.close();
		return c;
	}
	
	
	
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		Option jarOption = new Option("j", "jar", true, "path to topology jar file");
		jarOption.setRequired(true);
		options.addOption(jarOption);
		Option classOption = new Option("c", "class", true, "toplogy class name");
		classOption.setRequired(true);
		options.addOption(classOption);
		CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            String jarPath = cmd.getOptionValue("jar");
    		String topologyClass = cmd.getOptionValue("class");
    		Class c = loadJarFileClass(jarPath,topologyClass);
    		String[] newargs = cmd.getArgs();
    		Method cmain = c.getMethod("main", String[].class);
    		cmain.invoke(cmain, (Object) newargs);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("dragon -j jarPath -c className [args]", options);
            System.exit(1);
        }
		
		
	}

}
