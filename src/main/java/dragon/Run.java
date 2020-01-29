package dragon;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
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
import dragon.process.ProcessManager;
import dragon.tuple.RecycleStation;

/**
 * Main entry point for Dragon nodes.
 * @author aaron
 *
 */
public class Run {
	private static Log log = LogFactory.getLog(Run.class);
	private static ProcessManager pm; 
	private static int waitingFor=0;
	@SuppressWarnings("rawtypes")
	private static Class loadJarFileClass(String filePath, String className) throws ClassNotFoundException, IOException  {
		File f = new File(filePath);
		URLClassLoader classLoader = new URLClassLoader(new URL[]{f.toURI().toURL()});
		Class c = classLoader.loadClass(className);
		classLoader.close();
		return c;
	}
	
	/**
	 * Put the supplied topology JAR file onto the class path and invoke the topology main method.
	 * @param cmd
	 * @param conf
	 * @throws ParseException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 */
	private static void submit(CommandLine cmd, Config conf) throws ParseException, IOException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
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
		@SuppressWarnings("rawtypes")
		Class c = loadJarFileClass(jarPath,topologyClass);
		String[] newargs = cmd.getArgs();
		File file = new File(jarPath);
    	DragonSubmitter.topologyJar = Files.readAllBytes(file.toPath());
		@SuppressWarnings("unchecked")
		Method cmain = c.getMethod("main", String[].class);
		cmain.invoke(cmain, (Object) newargs);
	}
	
	/**
	 * dragon deploy [-h HOSTNAME] [-p DPORT] [-s SPORT] DRAGON-VERSION-distro.zip [USERNAME]
	 * Copy the package to each of the hosts in dragon.network.hosts, unzip it, prepare its
	 * configuration file with a copy of the locally used conf modified to suit the specific
	 * host, and put it online. If a host is given using -h then that host is specifically
	 * deployed to instead, with the -p and -s port overrides applying.
	 * @param cmd
	 * @param conf
	 * @throws ParseException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	private static void deploy(CommandLine cmd, Config conf) throws ParseException, IOException, InterruptedException {
		
		if(cmd.getArgs().length<2) {
			throw new ParseException("ERROR: a package distro must be given\n try: dragon deploy [-h HOSTNAME] [-p DPORT] [-s SPORT] DRAGON-VERSION-distro.zip [USERNAME]");
		}
		String distro = cmd.getArgList().get(1);
		String username=System.getProperty("user.name");
		if(cmd.getArgs().length==3) {
			username=cmd.getArgs()[2];
		}
		String hostname=null;
		if(cmd.hasOption("host")) {
			hostname=cmd.getOptionValue("host");
		}
		System.out.println("copying distro...");
		if(hostname!=null) {
			waitingFor++;
			scpdistro(hostname,username,distro);
		} else {
			for(HashMap<String,?> host : conf.getDragonNetworkHosts()) {
				String hostname2 = (String) host.get("hostname");
				if(hostname2==null) {
					System.out.println("an empty hostname was found in the configuration file: skipping");
					continue;
				}
				waitingFor++;
				scpdistro(hostname2,username,distro);
			}
		}
		while(waitingFor>0) {
			Thread.sleep(100);
		}
		System.out.println("unzipping distro...");
		if(hostname!=null) {
			waitingFor++;
			sshunzipdistro(hostname,username,distro);
		} else {
			for(HashMap<String,?> host : conf.getDragonNetworkHosts()) {
				String hostname2 = (String) host.get("hostname");
				if(hostname2==null) {
					System.out.println("an empty hostname was found in the configuration file: skipping");
					continue;
				}
				waitingFor++;
				sshunzipdistro(hostname2,username,distro);
			}
		}
		while(waitingFor>0) {
			Thread.sleep(100);
		}
		System.out.println("configuring...");
		Config tconf = new Config(conf);
		if(cmd.hasOption("dport")) {
			tconf.put(Config.DRAGON_NETWORK_LOCAL_DATA_PORT,Integer.parseInt(cmd.getOptionValue("dport")));
		}
		if(cmd.hasOption("sport")) {
			tconf.put(Config.DRAGON_NETWORK_LOCAL_SERVICE_PORT,Integer.parseInt(cmd.getOptionValue("sport")));
		}	
		if(hostname!=null) {
			waitingFor++;
			tconf.put(Config.DRAGON_NETWORK_LOCAL_HOST,hostname);
			sshconfiguredistro(hostname,username,distro,tconf);
		} else {
			for(HashMap<String,?> host : conf.getDragonNetworkHosts()) {
				String hostname2 = (String) host.get("hostname");
				if(hostname2==null) {
					System.out.println("an empty hostname was found in the configuration file: skipping");
					continue;
				}
				waitingFor++;
				tconf.put(Config.DRAGON_NETWORK_LOCAL_HOST,hostname2);
				Config tconf2 = new Config(tconf);
				sshconfiguredistro(hostname2,username,distro,tconf2);
			}
		}
		while(waitingFor>0) {
			Thread.sleep(100);
		}
		System.out.println("starting daemons...");
		if(hostname!=null) {
			waitingFor++;
			sshonlinedistro(hostname,username,distro);
		} else {
			for(HashMap<String,?> host : conf.getDragonNetworkHosts()) {
				String hostname2 = (String) host.get("hostname");
				if(hostname2==null) {
					System.out.println("an empty hostname was found in the configuration file: skipping");
					continue;
				}
				waitingFor++;
				sshonlinedistro(hostname2,username,distro);
			}
		}
		while(waitingFor>0) {
			Thread.sleep(100);
		}
		System.out.println("done");
		
	}
	
	private static void scpdistro(String hostname,String username,String distro) {
		Path path = Paths.get(distro); 
		Path fileName = path.getFileName();
		String info="scp "+distro+" "+username+"@"+hostname+":"+fileName;
		ProcessBuilder pb = new ProcessBuilder("scp", distro, username+"@" + hostname + ":" + fileName);
		pm.startProcess(pb, false, (p)->{
			System.out.println("Running: "+info);
		}, (pb2)->{
			System.out.println("Could not start process: "+info);
			System.exit(-1);
		}, (p)->{
			if(p.exitValue()!=0) {
				System.out.println("Process returned ["+p.exitValue()+"]: "+info);
			} 
			waitingFor--;
		});
	}
	
	private static void sshunzipdistro(String hostname,String username,String distro) {
		Path path = Paths.get(distro); 
		Path fileName = path.getFileName();
		String info="ssh "+username+"@"+hostname+" \"unzip -o "+fileName+"\"";
		ProcessBuilder pb = new ProcessBuilder("ssh",username+"@" + hostname,"unzip -o "+fileName);
		pm.startProcess(pb, false, (p)->{
			System.out.println("Running: "+info);
		}, (pb2)->{
			System.out.println("Could not start process: "+info);
			System.exit(-1);
		}, (p)->{
			if(p.exitValue()!=0) {
				System.out.println("Process returned ["+p.exitValue()+"]: "+info);
			}
			waitingFor--;
		});
	}
	
	private static void sshconfiguredistro(String hostname,String username,String distro,Config conf) {
		Path path = Paths.get(distro); 
		Path fileName = path.getFileName();
		String baseName = fileName.toString().substring(0,fileName.toString().length() - 11);
		String info="<CONF> | ssh "+username+"@"+hostname+" cat > "+baseName+"/conf/dragon.yaml";
		ProcessBuilder pb = new ProcessBuilder("ssh",username+"@" + hostname,"cat > "+baseName+"/conf/dragon.yaml");
		pm.startProcess(pb, false, (p)->{
			System.out.println("Running: "+info);
			OutputStream stdin = p.getOutputStream();
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin));
			try {
				writer.write(conf.toYamlStringNice());
			} catch (IOException e) {
				System.out.println("Could not send conf to machine: "+info);
				System.exit(-1);
			} finally {
				try {
					writer.close();
				} catch (IOException e) {
					log.warn("error closing connection to machine: "+info);
				}
				try {
					stdin.close();
				} catch (IOException e) {
					log.warn("error closing connection to machine: "+info);
				}
			}
		}, (pb2)->{
			System.out.println("Could not start process: "+info);
			System.exit(-1);
		}, (p)->{
			if(p.exitValue()!=0) {
				System.out.println("Process returned ["+p.exitValue()+"]: "+info);
			}
			waitingFor--;
		});
	}
	
	private static void sshonlinedistro(String hostname,String username,String distro) {
		Path path = Paths.get(distro); 
		Path fileName = path.getFileName();
		String baseName = fileName.toString().substring(0,fileName.toString().length() - 11);
		String command="nohup "+baseName+"/bin/dragon.sh -d -C "+baseName+"/conf/dragon.yaml"+" > "+baseName+"/log/dragon.stdout 2> "+baseName+"/log/dragon.stderr &";
		String info="ssh "+username+"@"+hostname+" "+command;
		ProcessBuilder pb = new ProcessBuilder("ssh",username+"@" + hostname,command);
		pm.startProcess(pb, false, (p)->{
			System.out.println("Running: "+info);
		}, (pb2)->{
			System.out.println("Could not start process: "+info);
			System.exit(-1);
		}, (p)->{
			if(p.exitValue()!=0) {
				System.out.println("Process returned ["+p.exitValue()+"]: "+info);
			}
			waitingFor--;
		});
	}
	
	@SuppressWarnings({ "unchecked" })
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
		Option execOption = new Option("e","exec",true,"[daemon|metrics|terminate|resume|halt|list|allocate|deallocate|deploy]");
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
            pm = new ProcessManager(conf);
            
            /*
             * First check to see if we are submitting a topology using the
             * approach: dragon -j JARFILE -c CLASS TOPOLOGYNAME
             * In this case, the topology name is read by the CLASS that is 
             * declaring the topology and submitting it. If no topology name
             * is given then it is simply run in local cluster mode.
             */
            if(cmd.hasOption("jar") && cmd.hasOption("class")) {
            	submit(cmd,conf);
            } else {
            
	            /*
	             * Otherwise check for what command is being issued.
	             */
	            
	            String exec = "";
	            if(cmd.hasOption("exec")) {
	            	exec = cmd.getOptionValue("exec");
	            } else if(cmd.getArgs().length>0) {
	            	exec = cmd.getArgList().get(0);
	            } else {
	            	if(cmd.hasOption("metrics")) {
	            		exec="metrics";
	            	} else if(cmd.hasOption("terminate")) {
	            		exec="terminate";
	            	} else if(cmd.hasOption("resume")) {
	            		exec="resume";
	            	} else if(cmd.hasOption("halt")) {
	            		exec="halt";
	            	} else if(cmd.hasOption("list")) {
	            		exec="list";
	            	} else if(cmd.hasOption("daemon")) {
	            		exec="daemon";
	            	} 
	            }
	            
	            switch(exec) {
	            case "":
	            	throw new ParseException("no command was given");
	            case "submit":
	            	submit(cmd,conf);
	            	break;
	            case "deploy":
	            	deploy(cmd,conf);
	            	break;
	            case "allocate":
	            	break;
	            case "metrics":{
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
	    			break;
	            }
	            case "terminate":{
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
	    			break;
	            }
	            case "resume":{
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
	    			break;
	            }
	            case "halt":{
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
	    			break;
	            }
	            case "list":{
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
	    			break;
	            }
	            case "daemon":{
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
					break;
	            }
	            default:
	            	throw new ParseException("unknown command: "+exec);
	            }
            }
       
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            String help="To simply submit a topology to run in local mode: \n";
            help+="dragon -j YOUR_TOPOLOGY_JAR.jar -c YOUR.PACKAGE.TOPOLOGY\n\n";
            help+="To submit a topology to a Dragon daemon: \n";
            help+="dragon -h HOST_NAME -s SERVICE_PORT -j YOUR_TOPOLOGY_JAR.jar -c YOUR.PACKAGE.TOPOLOGY TOPOLOGY_NAME\n\n";
            help+="To start a Dragon daemon: \n";
            help+="dragon -d\n\n";
            help+="Other commands are listed below, see README.md";
            formatter.printHelp(help, options);
            System.exit(1);
        } finally {
        	pm.interrupt();
        }
		
     
		
	}

}
