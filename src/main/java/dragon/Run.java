package dragon;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yaml.snakeyaml.Yaml;

import dragon.network.Node;
import dragon.tuple.RecycleStation;

public class Run {
	private static Log log = LogFactory.getLog(Run.class);
	
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
		final Properties properties = new Properties();
		properties.load(Run.class.getClassLoader().getResourceAsStream("project.properties"));
		log.info("dragon version "+properties.getProperty("project.version"));
		
		Options options = new Options();
		Option jarOption = new Option("j", "jar", true, "path to topology jar file");
		options.addOption(jarOption);
		Option classOption = new Option("c", "class", true, "toplogy class name");
		options.addOption(classOption);
		Option daemonOption = new Option("d", "daemon", false, "start as a daemon");
		options.addOption(daemonOption);
		Option nodeOption = new Option("h","host",true,"host name override");
		options.addOption(nodeOption);
		Option portOption = new Option("p","port",true,"data port override");
		options.addOption(portOption);
		Option sportOption = new Option("s","sport",true,"service port override");
		options.addOption(sportOption);
		Option metricsOption = new Option("m","metrics",false,"obtain metrics from existing node");
		options.addOption(metricsOption);
		Option topologyOption = new Option("t","topology",true,"name of the topology");
		options.addOption(topologyOption);
		Option terminateOption = new Option("X","terminate",false,"terminate a topology");
		options.addOption(terminateOption);
		Option resumeOption = new Option("r","resume",false,"resume a topology");
		options.addOption(resumeOption);
		Option haltOption = new Option("x","halt",false,"halt a topology");
		options.addOption(haltOption);
		Option listOption = new Option("l","list",false,"list topology information");
		options.addOption(listOption);
		Option confOption = new Option("C","conf",true,"specify the dragon conf file");
		options.addOption(confOption);
		Option execOption = new Option("e","exec",true,"[daemon|metrics|terminate|resume|halt|list|allocate|deallocate]");
		options.addOption(execOption);		
		
		
		CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        
        try {
            cmd = parser.parse(options, args);
            Config conf;
            if(cmd.hasOption("conf")) {
            	String val = cmd.getOptionValue("conf");
            	if(val.startsWith("{")) {
            		Yaml config = new Yaml();
            		conf = new Config((Map<String,Object>) config.load(val));
            	} else {
            		conf = new Config(val);
            	}
            } else {
            	conf = new Config(Constants.DRAGON_PROPERTIES);
            }
            RecycleStation.instanceInit(conf);
            
            String exec = "";
            if(cmd.hasOption("exec")) {
            	exec = cmd.getOptionValue("exec");
            }
            
            if(exec.equals("allocate")) {
            	
            } else if(exec.equals("deallocate")) {
            	
            } else if(exec.equals("metrics") || cmd.hasOption("metrics")){
            	DragonSubmitter.node = conf.getLocalHost();
    			if(cmd.hasOption("host")) {
    				DragonSubmitter.node.setHost(cmd.getOptionValue("host"));
    			}
    			if(cmd.hasOption("sport")) {
    				DragonSubmitter.node.setServicePort(Integer.parseInt(cmd.getOptionValue("sport")));
    			}
    			if(cmd.hasOption("port")) {
    				log.warn("the -p option was given but metrics does not use that option");
    			}
    			if(!cmd.hasOption("topology")){
    				throw new ParseException("must provide a topology name with -t option");
    			}
    			DragonSubmitter.getMetrics(conf,cmd.getOptionValue("topology"));
            } else if(exec.equals("terminate") || cmd.hasOption("terminate")){
            	DragonSubmitter.node = conf.getLocalHost();
    			if(cmd.hasOption("host")) {
    				DragonSubmitter.node.setHost(cmd.getOptionValue("host"));
    			}
    			if(cmd.hasOption("sport")) {
    				DragonSubmitter.node.setServicePort(Integer.parseInt(cmd.getOptionValue("sport")));
    			}
    			if(cmd.hasOption("port")) {
    				log.warn("the -p option was given but terminate does not use that option");
    			}
    			if(!cmd.hasOption("topology")){
    				throw new ParseException("must provide a topology name with -t option");
    			}
    			DragonSubmitter.terminateTopology(conf,cmd.getOptionValue("topology"));
            } else if(exec.equals("resume") || cmd.hasOption("resume")){
            	DragonSubmitter.node = conf.getLocalHost();
    			if(cmd.hasOption("host")) {
    				DragonSubmitter.node.setHost(cmd.getOptionValue("host"));
    			}
    			if(cmd.hasOption("sport")) {
    				DragonSubmitter.node.setServicePort(Integer.parseInt(cmd.getOptionValue("sport")));
    			}
    			if(cmd.hasOption("port")) {
    				log.warn("the -p option was given but resume does not use that option");
    			}
    			if(!cmd.hasOption("topology")){
    				throw new ParseException("must provide a topology name with -r option");
    			}
    			DragonSubmitter.resumeTopology(conf,cmd.getOptionValue("topology"));
            } else if(exec.equals("halt") || cmd.hasOption("halt")){
            	DragonSubmitter.node = conf.getLocalHost();
    			if(cmd.hasOption("host")) {
    				DragonSubmitter.node.setHost(cmd.getOptionValue("host"));
    			}
    			if(cmd.hasOption("sport")) {
    				DragonSubmitter.node.setServicePort(Integer.parseInt(cmd.getOptionValue("sport")));
    			}
    			if(cmd.hasOption("port")) {
    				log.warn("the -p option was given but halt does not use that option");
    			}
    			if(!cmd.hasOption("topology")){
    				throw new ParseException("must provide a topology name with -x option");
    			}
    			DragonSubmitter.haltTopology(conf,cmd.getOptionValue("topology"));
            } else if(exec.equals("list") || cmd.hasOption("list")){
            	DragonSubmitter.node = conf.getLocalHost();
    			if(cmd.hasOption("host")) {
    				DragonSubmitter.node.setHost(cmd.getOptionValue("host"));
    			}
    			if(cmd.hasOption("sport")) {
    				DragonSubmitter.node.setServicePort(Integer.parseInt(cmd.getOptionValue("sport")));
    			}
    			if(cmd.hasOption("port")) {
    				log.warn("the -p option was given but list does not use that option");
    			}
    			DragonSubmitter.listTopologies(conf);
            } else if(!cmd.hasOption("daemon") && !exec.equals("daemon")){
            	DragonSubmitter.node = conf.getLocalHost();
    			if(cmd.hasOption("host")) {
    				DragonSubmitter.node.setHost(cmd.getOptionValue("host"));
    			}
    			if(cmd.hasOption("sport")) {
    				DragonSubmitter.node.setServicePort(Integer.parseInt(cmd.getOptionValue("sport")));
    			}
    			if(cmd.hasOption("port")) {
    				log.warn("the -p option was given but submission does not use that option");
    			}
	            if(!cmd.hasOption("jar") || !cmd.hasOption("class")){
	            	throw new ParseException("must provide a jar file and class to run");
	            }
            	String jarPath = cmd.getOptionValue("jar");
	    		String topologyClass = cmd.getOptionValue("class");
	    		Agent.addToClassPath(new File(jarPath));
	    		//addClassPath(jarPath);
	    		Class c = loadJarFileClass(jarPath,topologyClass);
	    		String[] newargs = cmd.getArgs();
	    		File file = new File(jarPath);
            	DragonSubmitter.topologyJar = Files.readAllBytes(file.toPath());
	    		Method cmain = c.getMethod("main", String[].class);
	    		cmain.invoke(cmain, (Object) newargs);
            } else {
    			if(cmd.hasOption("host")) {
    				conf.put(Config.DRAGON_NETWORK_LOCAL_HOST, cmd.getOptionValue("host"));
    			}
    			if(cmd.hasOption("port")) {
    				conf.put(Config.DRAGON_NETWORK_LOCAL_DATA_PORT,Integer.parseInt(cmd.getOptionValue("port")));
    			}
    			if(cmd.hasOption("sport")) {
    				conf.put(Config.DRAGON_NETWORK_LOCAL_SERVICE_PORT, Integer.parseInt(cmd.getOptionValue("sport")));
    			}
            	log.info("starting dragon daemon");
            	
				new Node(conf);

            }
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("see the README.md file for usage information", options);
            System.exit(1);
        }
		
		
	}

}
