package dragon;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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

import dragon.network.Node;
import dragon.network.NodeDescriptor;

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
	private static URLClassLoader loadJarFile(String filePath) throws Exception {
	 
	    ArrayList<Class> availableClasses = new ArrayList<Class>();
	     
	    ArrayList<String> classNames = getClassNamesFromJar(filePath);
	    File f = new File(filePath);
	    URLClassLoader classLoader = new URLClassLoader(new URL[]{f.toURI().toURL()});
	    for (String className : classNames) {
	        try {
	            Class cc = classLoader.loadClass(className);
	            if(cc!=null) {
		            availableClasses.add(cc);
		            try {
		            	log.debug("loaded class "+cc.getCanonicalName()+" ");
		            } catch (InternalError e) {
		           
		            } catch (IncompatibleClassChangeError e) {
		            	
		            }
	            }
	        } catch (ClassNotFoundException e) {
	            //log.warn("Class " + className + " was not found! "+e.toString());
	        } catch (NoClassDefFoundError e) {
	        	//log.warn("Class definition for " + className + " was not found! "+e.toString());
	        } catch (IllegalAccessError e) {
	        	//log.warn("Illegal access error for " + className +": "+e.toString());
	        } catch (UnsupportedClassVersionError e) {
	        	//log.warn("Unsupported class version error for " + className +": "+e.toString());
	        }
	    }
	    classLoader.close();
	    return classLoader;
	}
	
	private static boolean addClassPath(String filePath) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, MalformedURLException {
		File f = new File(filePath);
		ClassLoader cl = ClassLoader.getSystemClassLoader();
		Method m = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        m.setAccessible(true);
        m.invoke(cl, (Object)f.toURI().toURL());
        return true;
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
		Config conf = new Config(Constants.DRAGON_PROPERTIES);
		Options options = new Options();
		Option jarOption = new Option("j", "jar", true, "path to topology jar file");
		jarOption.setRequired(false);
		options.addOption(jarOption);
		Option classOption = new Option("c", "class", true, "toplogy class name");
		classOption.setRequired(false);
		options.addOption(classOption);
		Option daemonOption = new Option("d", "daemon", false, "start as a daemon");
		daemonOption.setRequired(false);
		options.addOption(daemonOption);
		Option nodeOption = new Option("h","host",true,"hostname of existing node");
		nodeOption.setRequired(false);
		options.addOption(nodeOption);
		Option portOption = new Option("p","port",true,"port number of existing node");
		portOption.setRequired(false);
		options.addOption(portOption);
		
		CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        
        try {
            cmd = parser.parse(options, args);
            DragonSubmitter.node = new NodeDescriptor((String)conf.get(Config.DRAGON_NETWORK_MAIN_NODE),
    				(Integer)conf.get(Config.DRAGON_NETWORK_SERVICE_PORT));
			if(cmd.hasOption("host")) {
				DragonSubmitter.node.setHost(cmd.getOptionValue("host"));
			}
			if(cmd.hasOption("port")) {
				DragonSubmitter.node.setPort(Integer.parseInt(cmd.getOptionValue("port")));
			}
            if(!cmd.hasOption("daemon")){
	            if(!cmd.hasOption("jar") || !cmd.hasOption("class")){
	            	throw new ParseException("must provide a jar file and class to run");
	            }
            	String jarPath = cmd.getOptionValue("jar");
	    		String topologyClass = cmd.getOptionValue("class");
	    		addClassPath(jarPath);
	    		Class c = loadJarFileClass(jarPath,topologyClass);
	    		String[] newargs = cmd.getArgs();
	    		
	    		Method cmain = c.getMethod("main", String[].class);
	    		cmain.invoke(cmain, (Object) newargs);
            } else {
            	if(cmd.hasOption("host")){
            		log.info("starting dragon node and joining to "+cmd.getOptionValue("host"));
            		new Node(DragonSubmitter.node,conf);
            	} else {
	            	log.info("starting dragon node");
	            	new Node(conf);
            	}
            }
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("dragon [-d] [-h host] [-p port] [-j jarPath -c className [args]]", options);
            System.exit(1);
        }
		
		
	}

}