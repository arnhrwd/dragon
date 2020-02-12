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
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.yaml.snakeyaml.Yaml;

import dragon.network.Node;
import dragon.process.ProcessManager;
import dragon.tuple.RecycleStation;

/**
 * Main entry point for Dragon nodes. Parses the command line and 
 * runs appropriate commands.
 * @author aaron
 *
 */
public class Run {
	private static Logger log = LogManager.getLogger(Run.class);
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
	 * Utility class to change where the log file is sent.
	 * @param logFile the place to write the log file
	 */
	public static void updateLog4jConfiguration(String logFile) { 
		Properties props = System.getProperties();
		props.setProperty("logFile",logFile);
	    LoggerContext context = (LoggerContext)LogManager.getContext(false);
	    context.reconfigure();
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
	 * Utility function
	 * @param cmd
	 * @param conf
	 * @return a list of <i>unique</i> hostnames in order found in the conf file,
	 * or, if supplied with -h, the hostname as given  on the command line
	 */
	private static ArrayList<String> hostnames(CommandLine cmd, Config conf){
		ArrayList<String> hostnames = new ArrayList<String>();
		if(cmd.hasOption("host")) {
			hostnames.add(cmd.getOptionValue("host"));
		} else {
			String hostname="";
			for(HashMap<String,?> host : conf.getDragonNetworkHosts()) {
				if(!host.containsKey("hostname")) {
					System.out.println("an empty hostname was found in the configuration file: skipping");
					continue;
				} else {
					String nextHostname = (String) host.get("hostname");
					if(!nextHostname.equals(hostname)) {
						hostnames.add(nextHostname);
					}
					hostname=nextHostname;
				}
				
			}
		}
		return hostnames;
	}
	
	/**
	 * dragon deploy [-h HOSTNAME] [-p DPORT] [-s SPORT] DRAGON-VERSION-distro.zip [USERNAME]
	 * Setup a machine to make it ready for dragon. Installs java 11 and unzip using apt.
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
	private static void deploy(CommandLine cmd, Config conf,List<String> argList) throws ParseException, IOException, InterruptedException {
		
		if(argList.size()<1) {
			throw new ParseException("ERROR: a package distro must be given\n try: dragon deploy [-h HOSTNAME] [-p DPORT] [-s SPORT] DRAGON-VERSION-distro.zip [USERNAME]");
		}
		String distro = argList.get(0);
		String username=System.getProperty("user.name");
		if(argList.size()==2) {
			username=cmd.getArgs()[1];
		}
		ArrayList<String> hostnames = hostnames(cmd,conf);
		setup(hostnames,username,conf);
		distro(hostnames,username,distro,conf);
		configuredistro(hostnames,username,cmd,conf);
		onlinedistro(hostnames,username,cmd,conf);
	}
	
	/**
	 * Setup a set of Ubuntu machines with appropriate software.
	 * @param hostnames list of hostnames to ssh to
	 * @param username the username to login as
	 * @param conf provides the distro base dir to use
	 * @throws InterruptedException
	 */
	private static void setup(ArrayList<String> hostnames,String username,Config conf) throws InterruptedException {
		System.out.println("setting up machines...");
		for(String hostname : hostnames) {
			waitingFor++;
			sshsetup(hostname,username,conf.getDragonDeployDir());
		}
		while(waitingFor>0) {
			Thread.sleep(100);
		}
	}
	
	/**
	 * Copy a distribution to a set of Ubuntu machines.
	 * @param hostnames list of hostnames to scp to
	 * @param username the username to login as
	 * @param distro the name of the package, which must be a 
	 * dragon package ending in -distro.zip | -distro.tar.gz | -distro.tar.bz2
	 * @param conf provides the distro base dir to use
	 * @throws InterruptedException
	 */
	private static void distro(ArrayList<String> hostnames,String username,String distro,Config conf) throws InterruptedException {
		System.out.println("copying distro...");
		for(String hostname: hostnames) {
			waitingFor++;
			scpdistro(hostname,username,distro,conf.getDragonDeployDir());
		}
		while(waitingFor>0) {
			Thread.sleep(100);
		}
		// also unzip it
		unzipdistro(hostnames,username,distro,conf);
	}
	
	/**
	 * Unzip a distribution on a set of Ubuntu machines.
	 * @param hostnames list of hostnames to ssh to
	 * @param username the username to login as
	 * @param distro the name of the package, which must be a 
	 * dragon package ending in -distro.zip | -distro.tar.gz | -distro.tar.bz2
	 * @param conf provides the distro base dir to use
	 * @throws InterruptedException
	 */
	private static void unzipdistro(ArrayList<String> hostnames,String username,String distro,Config conf) throws InterruptedException {
		System.out.println("unzipping distro...");
		for(String hostname: hostnames) {
			waitingFor++;
			sshunzipdistro(hostname,username,distro,conf.getDragonDeployDir());
		}
		while(waitingFor>0) {
			Thread.sleep(100);
		}
	}
	
	/**
	 * Utility function.
	 * @param cmd
	 * @param conf
	 * @param i
	 * @return a conf that is specific to a given host
	 */
	private static Config specificConf(CommandLine cmd,Config conf,int i) {
		Config tconf = new Config(conf);
		ArrayList<String> hostnames = hostnames(cmd,conf);
		if(cmd.hasOption("dport")) {
			tconf.put(Config.DRAGON_NETWORK_LOCAL_DATA_PORT,Integer.parseInt(cmd.getOptionValue("dport")));
		}
		if(cmd.hasOption("sport")) {
			tconf.put(Config.DRAGON_NETWORK_LOCAL_SERVICE_PORT,Integer.parseInt(cmd.getOptionValue("sport")));
		}	
		if(cmd.hasOption("host")) {
			tconf.put(Config.DRAGON_NETWORK_LOCAL_HOST,hostnames.get(0));
		} else {
			HashMap<String,?> host = conf.getDragonNetworkHosts().get(i);
			tconf.put(Config.DRAGON_NETWORK_LOCAL_HOST,(String) host.get("hostname"));
			if(host.containsKey("dport")) {
				tconf.put(Config.DRAGON_NETWORK_LOCAL_DATA_PORT,(Integer)host.get("dport"));
			}
			if(host.containsKey("sport")) {
				tconf.put(Config.DRAGON_NETWORK_LOCAL_SERVICE_PORT,(Integer)host.get("sport"));
			}
			tconf.put(Config.DRAGON_NETWORK_PRIMARY,true);
			tconf.put(Config.DRAGON_NETWORK_PARTITION,Constants.DRAGON_PRIMARY_PARTITION);
			if(host.containsKey("partition")) {
				tconf.put(Config.DRAGON_NETWORK_PARTITION,(String)host.get("partition"));
			}
		}
		return tconf;
	}
	
	/**
	 * Create configuration files for Dragon daemons on a set of Ubuntu machines.
	 * @param hostnames list of hostnames to ssh to
	 * @param username the username to login as
	 * @param cmd is the command line parameters
	 * @param conf provides the default conf to use, which is overridden with the specific
	 * port numbers and hostname for each deamon
	 * @throws InterruptedException
	 */
	private static void configuredistro(ArrayList<String> hostnames,String username,CommandLine cmd, Config conf) throws InterruptedException {
		System.out.println("configuring...");
		if(cmd.hasOption("host")) {
			waitingFor++;
			sshconfiguredistro(hostnames.get(0),username,specificConf(cmd,conf,0));
		} else {
			int i=0;
			for(HashMap<String,?> host : conf.getDragonNetworkHosts()) {
				String hostname2 = (String) host.get("hostname");
				if(hostname2==null) {
					System.out.println("an empty hostname was found in the configuration file: skipping");
					i++;
					continue;
				}
				waitingFor++;
				sshconfiguredistro(hostname2,username,specificConf(cmd,conf,i));
				i++;
			}
		}
		while(waitingFor>0) {
			Thread.sleep(100);
		}
	}
	
	/**
	 * Bring online, using nohup, Dragon daemons on a set of Ubuntu machines.
	 * @param hostnames list of hostnames to ssh to
	 * @param username the username to login as
	 * @param cmd is the command line parameters
	 * @param conf provides the host information for each host to bring online
	 * @throws InterruptedException
	 */
	private static void onlinedistro(ArrayList<String> hostnames,String username,CommandLine cmd,Config conf) throws InterruptedException {
		System.out.println("bringing Dragon daemons online...");
		if(cmd.hasOption("host")) {
			waitingFor++;
			sshonlinedistro(hostnames.get(0),username,specificConf(cmd,conf,0));
		} else {
			int i=0;
			for(HashMap<String,?> host : conf.getDragonNetworkHosts()) {
				String hostname2 = (String) host.get("hostname");
				if(hostname2==null) {
					System.out.println("an empty hostname was found in the configuration file: skipping");
					i++;
					continue;
				}
				waitingFor++;
				sshonlinedistro(hostname2,username,specificConf(cmd,conf,i));
				i++;
			}
		}
		while(waitingFor>0) {
			Thread.sleep(100);
		}
	}
	
	/**
	 * Bring offline, using kill, Dragon daemons on a set of Ubuntu machines.
	 * @param hostnames list of hostnames to ssh to
	 * @param username the username to login as
	 * @param cmd is the command line parameters
	 * @param conf provides the host information for each host to bring offline
	 * @throws InterruptedException
	 */
	private static void offlinedistro(ArrayList<String> hostnames,String username,CommandLine cmd,Config conf) throws InterruptedException {
		System.out.println("bringing Dragon daemons offline...");
		if(cmd.hasOption("host")) {
			waitingFor++;
			sshofflinedistro(hostnames.get(0),username,specificConf(cmd,conf,0));
		} else {
			int i=0;
			for(HashMap<String,?> host : conf.getDragonNetworkHosts()) {
				String hostname2 = (String) host.get("hostname");
				if(hostname2==null) {
					System.out.println("an empty hostname was found in the configuration file: skipping");
					i++;
					continue;
				}
				waitingFor++;
				sshofflinedistro(hostname2,username,specificConf(cmd,conf,i));
				i++;
			}
		}
		while(waitingFor>0) {
			Thread.sleep(100);
		}
	}
	
	/**
	 * Utility function to ssh setup a Ubuntu machine.
	 * @param hostname
	 * @param username
	 * @param base
	 */
	private static void sshsetup(String hostname,String username,String base) {
		String info="ssh -oStrictHostKeyChecking=no "+username+"@"+hostname+
				" \"mkdir -p "+base+
				" && sudo apt update && sudo apt install -y openjdk-11-jre-headless unzip\"";
		ProcessBuilder pb = new ProcessBuilder("ssh","-oStrictHostKeyChecking=no",username+"@" + hostname,
				"mkdir -p "+base+" && sudo apt update && sudo apt install -y openjdk-11-jre-headless unzip");
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
	
	/**
	 * Utility function to scp a distro to a Ubuntu machine.
	 * @param hostname
	 * @param username
	 * @param distro
	 * @param base
	 */
	private static void scpdistro(String hostname,String username,String distro,String base) {
		Path path = Paths.get(distro); 
		Path fileName = path.getFileName();
		String info="scp -oStrictHostKeyChecking=no "+distro+" "+username+"@"+hostname+":"+base+"/"+fileName;
		ProcessBuilder pb = new ProcessBuilder("scp","-oStrictHostKeyChecking=no", distro,
				username+"@" + hostname + ":" + base+"/"+fileName);
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
	
	/**
	 * Utility function to remove the archive suffix from a distro filename.
	 * @param fileName
	 * @return the stripped filename
	 */
	private static String removeArchiveSuffix(String fileName) {
		if(fileName.endsWith("-distro.zip")) {
			return fileName.toString().substring(0,fileName.toString().length() - 11);
		} else if(fileName.endsWith("-distro.tar.gz")) {
			return fileName.toString().substring(0,fileName.toString().length() - 14);
		} else if(fileName.endsWith("-distro.tar.bz2")) {
			return fileName.toString().substring(0,fileName.toString().length() - 15);
		} else {
			System.out.println("The distro must be one of *-distro.zip | *-distro.tar.gz | *-distro.tar.bz2");
			System.exit(-1);
		}
		return null;
	}
	
	/**
	 * Utility function to return the command that will unpack the distro
	 * @param fileName
	 * @return the command that will unpack the distro
	 */
	private static String uncompressCommand(String fileName) {
		if(fileName.endsWith("-distro.zip")) {
			return "unzip -o";
		} else if(fileName.endsWith("-distro.tar.gz")) {
			return "tar xfa";
		} else if(fileName.endsWith("-distro.tar.bz2")) {
			return "tar xfa";
		} else {
			System.out.println("The distro must be one of *-distro.zip | *-distro.tar.gz | *-distro.tar.bz2");
			System.exit(-1);
		}
		return null;
	}
	
	/**
	 * Utility function to ssh into a Ubuntu machine an unzip a distro.
	 * @param hostname
	 * @param username
	 * @param distro
	 * @param base
	 */
	private static void sshunzipdistro(String hostname,String username,String distro,String base) {
		Path path = Paths.get(distro); 
		Path fileName = path.getFileName();
		String baseName = removeArchiveSuffix(fileName.toString());
		String info="ssh -oStrictHostKeyChecking=no "+username+"@"+hostname+" \"cd "+base+
				" && "+uncompressCommand(fileName.toString())+" "+fileName+" && rm -f dragon && ln -s "+baseName+" dragon\"";
		ProcessBuilder pb = new ProcessBuilder("ssh","-oStrictHostKeyChecking=no",username+
				"@" + hostname,"cd "+base+" && "+uncompressCommand(fileName.toString())+" "+fileName+" && rm -f dragon && ln -s "+baseName+" dragon");
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
	
	/**
	 * Utility function to ssh into a Ubuntu machine and configure a Dragon daemon.
	 * @param hostname
	 * @param username
	 * @param conf
	 */
	private static void sshconfiguredistro(String hostname,String username,Config conf) {
		String info="<CONF> | ssh -oStrictHostKeyChecking=no "+username+
				"@"+hostname+" \"cat > "+conf.getDragonDeployDir()+"/dragon/conf/dragon-"+conf.getDragonNetworkLocalDataPort()+".yaml\"";
		ProcessBuilder pb = new ProcessBuilder("ssh","-oStrictHostKeyChecking=no",
				username+"@" + hostname,"cat > "+conf.getDragonDeployDir()+"/dragon/conf/dragon-"+conf.getDragonNetworkLocalDataPort()+".yaml");
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
	
	/**
	 * Utility function to ssh into a Ubuntu machine and bring a Dragon daemon online.
	 * @param hostname
	 * @param username
	 * @param conf
	 */
	private static void sshonlinedistro(String hostname,String username,Config conf) {
		String base=conf.getDragonDeployDir();
		String command="nohup "+base+"/dragon/bin/dragon.sh -d -C "+base+"/dragon/conf/dragon-"+conf.getDragonNetworkLocalDataPort()+".yaml"+" > "+
				base+"/dragon/log/dragon-"+conf.getDragonNetworkLocalDataPort()+".stdout 2> "+base+"/dragon/log/dragon-"+conf.getDragonNetworkLocalDataPort()+".stderr &";
		String info="ssh -oStrictHostKeyChecking=no "+username+"@"+hostname+" \""+command+"\"";
		ProcessBuilder pb = new ProcessBuilder("ssh","-oStrictHostKeyChecking=no",username+"@" + hostname,command);
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
	
	/**
	 * Utility function to ssh into a Ubuntu machine and bring a Dragon daemon offline.
	 * @param hostname
	 * @param username
	 * @param conf
	 */
	private static void sshofflinedistro(String hostname,String username,Config conf) { 
		String command="kill `cat "+conf.getDragonDataDir()+"/dragon-"+conf.getDragonNetworkLocalDataPort()+".pid`";
		String info="ssh -oStrictHostKeyChecking=no "+username+"@"+hostname+" \""+command+"\"";
		ProcessBuilder pb = new ProcessBuilder("ssh","-oStrictHostKeyChecking=no",username+"@" + hostname,command);
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
	
	/**
	 * Utility class to get the configuration.
	 * @param cmd
	 * @param logon
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private static Config getConf(CommandLine cmd,String exec,boolean logon) throws IOException {
		Config conf;
        if(cmd.hasOption("conf")) {
        	String val = cmd.getOptionValue("conf");
        	if(val.startsWith("{")) {
        		Yaml config = new Yaml();
        		conf = new Config((Map<String,Object>) config.load(val));
        		if(logon) log.debug("using conf "+conf.toYamlString());
        	} else {
        		conf = new Config(val,logon);
        	}
        } else {
        	conf = new Config(Constants.DRAGON_PROPERTIES,logon);
        }
        if(exec.equals("daemon")) {
        	if(cmd.hasOption("host")) {
				conf.put(Config.DRAGON_NETWORK_LOCAL_HOST, cmd.getOptionValue("host"));
			}
			if(cmd.hasOption("port")) {
				conf.put(Config.DRAGON_NETWORK_LOCAL_DATA_PORT,Integer.parseInt(cmd.getOptionValue("port")));
			}
			if(cmd.hasOption("sport")) {
				conf.put(Config.DRAGON_NETWORK_LOCAL_SERVICE_PORT, Integer.parseInt(cmd.getOptionValue("sport")));
			}
        }
        return conf;
	}
	
	/**
	 * Utility function to get the host and sport parameter options if they exist.
	 * @param cmd
	 * @throws UnknownHostException
	 */
	private static void extractHostSportCombo(CommandLine cmd) throws UnknownHostException {
		if(cmd.hasOption("host")) {
			DragonSubmitter.node.setHost(cmd.getOptionValue("host"));
		}
		if(cmd.hasOption("sport")) {
			DragonSubmitter.node.setServicePort(Integer.parseInt(cmd.getOptionValue("sport")));
		}
	}
	
	/**
	 * Utility function to check options are given for commands that refer to a topology.
	 * @throws UnknownHostException 
	 * @throws ParseException 
	 * 
	 */
	private static void checkOptionsTopologyCommand(CommandLine cmd) throws UnknownHostException, ParseException {
		extractHostSportCombo(cmd);
		if(cmd.hasOption("port")) {
			log.warn("the -p option was given but terminate does not use that option");
		}
		if(!cmd.hasOption("topology")){
			throw new ParseException("must provide a topology name with this command, using the -t option");
		}
	}
	
	/**
	 * Utility function to check to the options for daemon commands.
	 * @param cmd
	 * @throws UnknownHostException
	 */
	private static void checkOptionsDaemonCommand(CommandLine cmd) throws UnknownHostException {
		extractHostSportCombo(cmd);
	}
	
	/**
	 * Main
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		Option helpOption = new Option("?","help",false,"help");
		options.addOption(helpOption);
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
		Option statusOption = new Option("st","status",false,"get status of the daemons");
		options.addOption(statusOption);
		Option execOption = new Option("e","exec",true,"[daemon|metrics|terminate|resume|halt|"
		+"list|allocate|deallocate|deploy|setup|distro|unzip|config|online|offline|status]");
		options.addOption(execOption);		
		
		
		CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        
        try {
        	List<String> argList = new ArrayList<String>();
            cmd = parser.parse(options, args);
            if(cmd.hasOption("help")) {
            	throw new ParseException("Help");
            }
            String exec = "";
            /*
             * First check to see if we are submitting a topology using the
             * approach: dragon -j JARFILE -c CLASS TOPOLOGYNAME
             * In this case, the topology name is read by the CLASS that is 
             * declaring the topology and submitting it. If no topology name
             * is given then it is simply run in local cluster mode.
             */
            if(cmd.hasOption("jar") && cmd.hasOption("class")) {
            	exec="submit";
            	if(cmd.getArgList().size()==1) {
            		if(cmd.getArgList().get(0).equals("submit")) {
            			System.out.println("WARNING: you appear to be submitting a topology but you may not have given a toplogy name. The name \"submit\" will be used.");
            		}
            	} else if(cmd.getArgList().size()>1) {
            		if(cmd.getArgList().get(0).equals("submit")) {
            			cmd.getArgList().remove(0);
            		}
            	}
            } else {
	            /*
	             * Otherwise check for what command is being issued.
	             */
	            if(cmd.hasOption("exec")) {
	            	exec = cmd.getOptionValue("exec");
	            	argList=cmd.getArgList();
	            } else if(cmd.getArgs().length>0) {
	            	exec = cmd.getArgList().get(0);
	            	argList=cmd.getArgList();
	            	argList.remove(0);
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
	            	} else if(cmd.hasOption("status")) {
	            		exec="status";
	            	}
	            }
            }
            /*
             * We are going to get the conf a first time, just to see whether
             * the user has specifically stated a log file destination or not.
             * We'll tell the config class to be silent, so it does not write
             * to a log in some undesired place.
             */
            Config conf = getConf(cmd,exec,false);
            Run.updateLog4jConfiguration(conf.getDragonLogDir()+"/dragon-"+conf.getDragonNetworkLocalDataPort());
            /*
             * Give a welcome message.
             */
            final Properties properties = new Properties();
    		properties.load(Run.class.getClassLoader().getResourceAsStream("project.properties"));
    		log.info("dragon version "+properties.getProperty("project.version"));
    		System.out.println("dragon version "+properties.getProperty("project.version"));
            /*
             * Now we get the conf again, but this time let it write stuff to the
             * log if it needs to.
             */
    		conf = getConf(cmd,exec,true);
    		/*
    		 * The recycle station needs to be initialized even if we're just
    		 * using the comms layer as a client.
    		 */
            RecycleStation.instanceInit(conf); 
            
            switch(exec) {
            case "":
            	throw new ParseException("no command was given");
            /*
             * SUBMIT A TOPOLOGY COMMAND
             */
            case "submit":
            	submit(cmd,conf);
            	break;
            /*
             * START A DAEMON COMMAND
             */
            case "daemon":{
            	log.info("starting dragon daemon");
				new Node(conf);
				break;
            }
            /*
             * DEPLOYMENT RELATED COMMANDS 
             * Such commands make use of scp/ssh
             */
            case "deploy":
            	pm = new ProcessManager(conf);
            	deploy(cmd,conf,argList);
            	pm.interrupt();
            	break;
            case "setup": {
            	pm = new ProcessManager(conf);
            	ArrayList<String> hostnames = hostnames(cmd,conf);
            	String username=System.getProperty("user.name");
        		if(cmd.getArgs().length==2) {
        			username=cmd.getArgs()[1];
        		}
            	setup(hostnames,username,conf);
            	pm.interrupt();
            	break;
            }
            case "distro":{
            	pm = new ProcessManager(conf);
            	ArrayList<String> hostnames = hostnames(cmd,conf);
            	if(argList.size()<1) {
        			throw new ParseException("ERROR: a package distro must be given\n try: dragon deploy [-h HOSTNAME] [-p DPORT] [-s SPORT] DRAGON-VERSION-distro.zip [USERNAME]");
        		}
        		String distro = argList.get(0);
        		String username=System.getProperty("user.name");
        		if(argList.size()==2) {
        			username=argList.get(1);
        		}
            	distro(hostnames,username,distro,conf);
            	pm.interrupt();
            	break;
            }
            case "unzip":{
            	pm = new ProcessManager(conf);
            	ArrayList<String> hostnames = hostnames(cmd,conf);
            	if(argList.size()<1) {
        			throw new ParseException("ERROR: a package distro must be given\n try: dragon deploy [-h HOSTNAME] [-p DPORT] [-s SPORT] DRAGON-VERSION-distro.zip [USERNAME]");
        		}
        		String distro = argList.get(0);
        		String username=System.getProperty("user.name");
        		if(argList.size()==2) {
        			username=argList.get(1);
        		}
            	unzipdistro(hostnames,username,distro,conf);
            	pm.interrupt();
            	break;
            }
            case "config":{
            	pm = new ProcessManager(conf);
            	ArrayList<String> hostnames = hostnames(cmd,conf);
            	String username=System.getProperty("user.name");
        		if(argList.size()==1) {
        			username=argList.get(0);
        		}
            	configuredistro(hostnames,username,cmd,conf);
            	pm.interrupt();
            	break;
            }
            case "online":{
            	pm = new ProcessManager(conf);
            	ArrayList<String> hostnames = hostnames(cmd,conf);
            	String username=System.getProperty("user.name");
        		if(argList.size()==1) {
        			username=argList.get(0);
        		}
            	onlinedistro(hostnames,username,cmd,conf);
            	pm.interrupt();
            	break;
            }
            case "offline":{
            	pm = new ProcessManager(conf);
            	ArrayList<String> hostnames = hostnames(cmd,conf);
            	String username=System.getProperty("user.name");
        		if(argList.size()==1) {
        			username=argList.get(0);
        		}
            	offlinedistro(hostnames,username,cmd,conf);
            	pm.interrupt();
            	break;
            }
            /*
             * DAEMON CONTROL COMMANDS
             */
            case "list":{
            	DragonSubmitter.node = conf.getLocalHost();
            	extractHostSportCombo(cmd);
    			DragonSubmitter.listTopologies(conf);
    			break;
            }
            case "allocate":
            	DragonSubmitter.node = conf.getLocalHost();
            	checkOptionsDaemonCommand(cmd);
            	DragonSubmitter.allocatePartition(conf,argList);
            	break;
            case "deallocate":
            	DragonSubmitter.node = conf.getLocalHost();
            	checkOptionsDaemonCommand(cmd);
            	DragonSubmitter.deallocatePartition(conf,argList);
            	break;
            case "status":
            	DragonSubmitter.node = conf.getLocalHost();
            	extractHostSportCombo(cmd);
    			DragonSubmitter.getStatus(conf);
    			break;
            	
            /*
             * TOPOLOGY RELATED COMMANDS
             * Those that require a -t option to give a specific topology name
             */
            case "metrics":{
            	DragonSubmitter.node = conf.getLocalHost();
    			checkOptionsTopologyCommand(cmd);
    			DragonSubmitter.getMetrics(conf,cmd.getOptionValue("topology"));
    			break;
            }
            case "terminate":{
            	DragonSubmitter.node = conf.getLocalHost();
    			checkOptionsTopologyCommand(cmd);
    			DragonSubmitter.terminateTopology(conf,cmd.getOptionValue("topology"));
    			break;
            }
            case "resume":{
            	DragonSubmitter.node = conf.getLocalHost();
    			checkOptionsTopologyCommand(cmd);
    			DragonSubmitter.resumeTopology(conf,cmd.getOptionValue("topology"));
    			break;
            }
            case "halt":{
            	DragonSubmitter.node = conf.getLocalHost();
    			checkOptionsTopologyCommand(cmd);
    			DragonSubmitter.haltTopology(conf,cmd.getOptionValue("topology"));
    			break;
            }
            
            
            default:
            	throw new ParseException("unknown command: "+exec);
            }
            
       
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            String help="To simply submit a topology to run in local mode: \n";
            help+="dragon.sh -j YOUR_TOPOLOGY_JAR.jar -c YOUR.PACKAGE.TOPOLOGY\n\n";
            help+="To submit a topology to a Dragon daemon: \n";
            help+="dragon.sh -h HOST_NAME -s SERVICE_PORT -j YOUR_TOPOLOGY_JAR.jar -c YOUR.PACKAGE.TOPOLOGY TOPOLOGY_NAME\n\n";
            help+="To start a Dragon daemon: \n";
            help+="dragon.sh -d\n\n";
            help+="Other commands are listed below, see README.md";
            formatter.printHelp(help, options);
            System.exit(1);
        } 
		
	}

}
