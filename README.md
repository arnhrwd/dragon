
# Dragon

A high performance, distributed stream processing system, loosely based on the Apache Storm API. One of the goals is to provide a *lightweight* stream processing engine - a single JVM on each machine/device will suffice, though multiple JVMs can be installed if you wish; there are no other third party systems required and the list of dependencies is quite small. The compressed distribution package is currently about 6.5MB. Dragon has a very small memory footprint and has tested well on edge devices like the Raspberry Pi. Another goal is to provide high performance and correctness of operation. Under fault-free conditions, data in the system is never dropped or lost. The architectural direction of Dragon tends towards decentralization - topologies can be remotely submitted to any node of the system. Similarly for service commands to manage the system. Fault tolerance currently only includes detecting faults and reporting them for user action - e.g. sufficient exceptions thrown by a topology will halt the topology, requiring the user to terminate or resume it, and dead JVM processes will be removed from the system with affected topologies being faulted and not automatically restarted. Future Dragon development will
- use check-pointing to automatically restart faulted topologies,
- provide a distributed in-memory key-value storage API for bolts and
- provide for out-of-band communication to support cycles.

## Code snippet example

If you are familiar with Apache Storm then you will likely find Dragon straight forward to make use of, e.g. a Spout might look like:

	public class TextSpout extends BaseRichSpout {
		private SpoutOutputCollector collector;
		
		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.collector=collector;
		}
		
		@Override
		public void nextTuple() {
			String text = possibly_get_some_text();
			if(text!=null)
				collector.emit(new Values(text));
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("text"));
		}
	}

Similarly a Bolt might look like:

	public class TextBolt extends BaseRichBolt {
		@Override
		public void execute(Tuple tuple) {
			String text = (Integer)tuple.getValueByField("text");
			do_some_processing(text);
		}
	}

With this, a topology might be declared like:

	public class Topology {
		public static void main(String[] args) throws InterruptedException {
			TopologyBuilder topologyBuilder = new TopologyBuilder();
			topologyBuilder.setSpout("textSpout", (Spout)new TextSpout(), 1).setNumTasks(1);
			topologyBuilder.setBolt("textBolt", (Bolt)new TextBolt(), 1).setNumTasks(1)
				.allGrouping("textSpout","text");
			Config conf = new Config();
			if(args.length==0) {
				LocalCluster localCluster = new LocalCluster();
				localCluster.submitTopology("numberTopology", conf, topologyBuilder.createTopology());
			} else {
				DragonSubmitter.submitTopology(args[0], conf, topologyBuilder.createTopology());
			}
		}
	}
	
There is currently *no* coding documentation for this project, apart from the rudimentary test topologies in `dragon.examples`. If you are familiar with Apache Storm, this project provides a limited subset of the Apache Storm API and the semantics of some aspects may differ. However it *should* be relatively straight forward to port an Apache Storm project to Dragon, especially if your Apache Storm project has limited itself to basic spouts and bolts. Please ask the Dragon developers for assistance if you want to port a project across. There are some changes and stipulations:

- `implements CustomStreamGrouping` becomes `extends AbstractGrouping`
- all topology objects **must** be serializable, as the entire topology is serialized when submitting it

# Distribution usage

The Dragon distribution has the following directory structure and relevant files:

    dragon
    |   LICENSE
    |   README.md
    └───bin
    |   |   dragon.sh
    |
    └───conf
    |   |   dragon.yaml
    |
    └───lib
    |   |   dragon.jar
    |
    └───log

To see simple help on using Dragon:

    cd dragon/bin
    ./dragon.sh --help
 
## Quick setup on a cluster

Dragon is recommended to run on a cluster of Ubuntu machines. Ensure that you have SSH access, preferably using a public key and key agent, to all Ubuntu machines upon which you want to deploy Dragon. You may deploy from your laptop/PC or from a main node of the cluster.

### Configure a dragon.yaml

Unpack the distribution into a directory that will not be the deployment directory. Configure a `dragon.yaml` file with the set of Ubuntu machines. In the example below we have 3 Ubuntu machines that will be deployed to. The `dragon.deploy.dir` is the directory to put the distribution installation in, on each Ubuntu machine - in this case the user is called `ubuntu` and the installation will be deployed within the `packages` dir in the user's home directory.

    dragon.network.hosts:
    - hostname: 192.168.1.1
    - hostname: 192.168.1.2
    - hostname: 192.168.1.3
    dragon.deploy.dir: /home/ubuntu/packages

Dragon has a number of deployment commands available, with `deploy` bundling a number of them together. It takes the original distro package as a command line parameter. **Note that these commands are destructive - they will overwrite whatever exists at the required locations.**

    ./dragon.sh deploy PATH_TO_DISTRO/dragon-VERSION-distro.zip [USERNAME]

The distro must be one of `-distro.zip`, `-distro.tar.gz` or `-distro.tar.bz2`. The `USERNAME` is an optional parameter that is the username to login to the Ubuntu machines as, otherwise the user's login name will be used. The `deploy` command will install software on each Ubuntu machine:

    mkdir -p /home/ubuntu/packages && sudo apt update && sudo apt install -y openjdk-11-jre-headless unzip

It will then copy the distribution into the deploy dir (`/home/ubuntu/packages` in this example), unpack it and configure it. The configuration file and other related files like log files will have the data port appended to them, so that they are unique when multiple Dragon daemons are on the same machine, e.g.:
    
    /home/ubuntu/packages
    |   dragon-VERSION-distro.zip
    └───dragon -> dragon-VERSION
    |   dragon-VERSION 
    |   └───conf
    |   |   |   dragon-4001.yaml

_Note that if you install the distribution manually, it MUST have the  dragon directory (or symbolic link) as above, that is the dragon home directory if you wish to use the config/online/offline commands._

It will also bring the daemons online using a command like:

    nohup /home/ubuntu/packages/dragon/bin/dragon.sh -d -C /home/ubuntu/packages/dragon/conf/dragon-4001.yaml > /home/ubuntu/packages/dragon/log/dragon-4001.stdout 2> /home/ubuntu/packages/dragon/log/dragon-4001.stderr &

You may use `supervisord` to keep these nodes online. In future versions the use of `supervisord` may become redundant, as daemons will have the capacity to restart each other as needed.
    
If you just wanted to copy a new distribution to the machines and unpack it, i.e. skipping the installation of JDK and such, then:

    ./dragon.sh distro PATH_TO_DISTRO/dragon-VERSION-distro.zip [USERNAME]
    ./dragon.sh config [USERNAME]

Now you can submit a topology to the Dragon cluster (see further below for detailed instructions):

    ./dragon.sh -h 192.168.1.1 -j YOUR_TOPOLOGY_JAR.jar -c YOUR.PACKAGE.TOPOLOGY TOPOLOGY_NAME

Other commands that are useful for controlling deployment are:

    ./dragon.sh offline [USERNAME]
    ./dragon.sh online [USERNAME]
    ./dragon.sh config [USERNAME]
    
These allow you to `kill` all Dragon daemons (bring them offline), bring them back online and to redo their configuration file, e.g. if you have changed parameters, etc. If you decide to add more machines to the cluster then it can be done incrementally to an existing set of nodes that are running (by deploying only to those specific nodes), or, somewhat more straight forwardly, all nodes can be offlined, reconfigured and brought back online together.

# Security

There is currently **NO** security with Dragon. Make sure to keep all Dragon daemons behind a firewall, away from public access. It is assumed that users of Dragon are trusted.

# Compiling

To install Dragon into your local cache:

    mvn install
    
## Dependency

Include the dependency in your project's `pom.xml`, where `VERSION` is replaced with whatever version you are using: 

    <dependency>
        <groupId>tech.streamsoft</groupId>
        <artifactId>dragon</artifactId>
        <version>VERSION</version>
        <scope>provided</scope>
    </dependency>
    
Use the `provided` scope when running in network mode. For local testing this should be removed.    

# Note about class loading in Java 11

User supplied JAR files are loaded into the Dragon daemon inside a class loader. When the topology is removed from the daemon, the class loader is dereferenced. Assuming that the user code is well-behaving, i.e. that any threads started are interrupted when components are asked to close, then all memory associated with the topology will be garbage collected. *The class loading strategy may cause some problems with third party libraries* that also interact with the class loader hierarchy, e.g. some yaml parsers do this and may or may not work within the Dragon class loader. If the library provides a hook for custom class loading then it may be possible to overcome this issue through static references to the Dragon class loader.

Dynamic addition of a JAR file to the class path was used by Dragon to load a topology class in the past, and all associated classes, from a supplied topology JAR file. The approach is to use the `instrumentation` package to achieve this, which requires a Java agent to load prior to the `main` method. Assuming `dragon.jar` is the JAR file with dependencies compiled in then running Dragon directly from the command line becomes:

    java -javaagent:dragon.jar -jar dragon.jar ...

While this is still required on the command line (if not using the supplied `dragon.sh`), it is deprecated and not used internally.
 
In this rest of this documentation, the above is simplified as just:

    dragon ...

# Local mode

Run:

    dragon -h

to provide help on options. To execute a topology in *local mode*:

    dragon -j YOUR_TOPOLOGY_JAR.jar -c YOUR.PACKAGE.TOPOLOGY

Running in local mode, Dragon creates a *local cluster* in a single JVM that contains all Spouts and Bolts without any need for networking. It does not create a Dragon daemon and therefore it cannot be connected to on a service port.

Local mode is useful for development and debugging. Alternatively, *Network mode* (described later) requires starting one or more Dragon daemons (typically on a cluster) that will connect to each other and allow topologies to be submitted via a service port to any selected Dragon daemon. Details on how to run Dragon in Network mode are provided after the Configuration details below, which describe how to configure Dragon in both Local mode and Network mode.

# Configuration

Dragon looks for `dragon.yaml` in several places, in this order:

- `./dragon.yaml`
- `${DRAGON_HOME}/conf/dragon.yaml`
- `/etc/dragon/dragon.yaml`
- `${HOME}/.dragon/dragon.yaml`

The first file found will be used to load the parameters. If you wish to specify the configuration file name on the command line then use:

    dragon -C YOUR_CONF.yaml ...

## Parameters

The available parameters and their defaults are listed below.

### General parameters

Spouts have output buffers, while Bolts have both input and output buffers. There is a separate buffer for _each_ declared stream out of each component and an input buffer for each bolt. Each buffer element stores a bundle of tuples. Therefore the maximum number of tuples on a buffer is the size of the buffer by the size of the tuple bundle. Each Spout or Bolt can have a bundle of tuples lingering for each output stream on a per task destination basis, in addition to the output buffers. The memory consumption is dominated by these parameters. Each component instance is allocated 1 thread. In addition there are some threads that help to transfer tuples between components.

- `dragon.output.buffer.size: 16` **Integer** - the size of the buffers on Spout and Bolt outputs
- `dragon.input.buffer.size: 16` **Integer** - the size of the buffers on Spout and Bolt inputs
- `dragon.tuple.bundle.size: 64` **Integer** - the number of tuples to bundle up for transmission, rather than transmitting tuples one at a time
- `dragon.tuple.bundle.linger.ms: 50` **Long** - the number of milliseconds that a tuple bundle, regardless of how many tuples it contains, can wait before being transmitted
- `dragon.localcluster.threads: 2 ` **Integer** - the size of the thread pool that transfers tuples within a local cluster

**Performance note:** While there are many factors that influence performance, as `dragon.tuple.bundle.size` is lowered, the number of context switches increases which can negatively impact performance significantly. Conversely if the value becomes large then the total amount of memory consumed increases, bearing in mind that the size of a tuple depends on the application.

### Network mode parameters

Buffer and thread resources:

- `dragon.router.input.threads: 1` **Integer** - the size of the thread pool that transfers tuples into the local cluster from the network (note that values larger than 1 result in tuple reordering on streams)
- `dragon.router.output.threads: 1` **Integer** - the size of the thread pool that transfers tuples out of the local cluster to the network (note that values large than 1 result in tuple reordering on streams)
- `dragon.router.input.buffer.size: 16` **Integer** - the size of the buffers for tuples transferring into the local cluster from the network
- `dragon.router.output.buffer.size: 16` **Integer** - the size of the buffers for tuples transferring out of the local cluster to the network

Comms and service details:

The retry and retry attempts, below, for socket connections are set for very short lived transient network failure, not for otherwise long term failure events. In the later case see the faults settings further below.

- `dragon.comms.retry.ms: 2000` **Integer** - the number of milliseconds to wait between retries when attempting to make a connection
- `dragon.comms.retry.attempts: 5` **Integer** - the number of retries to make before suspending retry attempts
- `dragon.comms.reset.count: 128` **Integer** - (advanced) the number of network tasks transmitted over an object stream before reseting the object stream handle table
- `dragon.comms.incoming.buffer.size: 1024` **Integer** - the size of the buffer for incoming network tasks, shared over all sockets
- `dragon.service.timeout.ms: 30000` **Integer** - the number of milliseconds to wait before a client service command will timeout and return with error

Network details:

- `dragon.network.default.service.port: 4000` **Integer** - the default service port
- `dragon.network.default.data.port: 4001` **Integer** - the default data port
- `dragon.network.local.host: localhost` **String** - the default advertised host name for the Dragon daemon
- `dragon.network.local.service.port: ` **Integer** - the service port for the Dragon daemon, if not set then the default service port is used 
- `dragon.network.local.data.port: ` **Integer** - the data port for the Dragon daemon, if not set then the default data port is used
- `dragon.network.hosts: [{hostname:localhost,dport:4001,sport:4000,primary:true,partition:primary},...]` **HostArray** - strictly an array of dictionaries, see below for details, which is used to bootstrap a daemon

The `dragon.network.hosts` parameter can also be written like this:

    dragon.network.hosts:
    - hostname: localhost
      dport: 4001
      sport: 4000
      primary: true
      partition: primary
    - hostname: host2.example.com

The `hostname` must be supplied otherwise the entry is skipped, omitted port values will take on default values, omitted `primary` will become `true`, and omitted `partition` will become `primary`.

Primary daemon and network partition (**This section is currently not supported**):

- `dragon.network.primary: true` **Boolean** - Dragon daemons listed in the `dragon.yaml` must all be listed as `true` (in fact, they will be set true anyway). A primary daemon is one which was set online by the command line interface. Non-primary daemons are created by primary daemons, when allocating new partitions and so on, as explained further in.
- `dragon.network.partition: primary` **String** - the partition name for this daemon

The primary daemon is responsible for starting up other daemons on the same machine, and monitoring them for liveness, restarting as needed. The primary daemon would usually be started via a remote `ssh` command. It is not necessary to use `supervisord` since the daemons will supervise each other.

Every Dragon daemon is part of a single partition, by default called the `primary` partition. When a topology is submitted, the embedding strategy can select the partition that the topology will be embedded into, which allows for complete process isolation of topologies.

Parameters concerning metrics:

- `dragon.metrics.enable: true` **Boolean** - whether the Dragon daemon should record metrics
- `dragon.metrics.sample.history: 1` **Integer** - how much sample history to record
- `dragon.metrics.sample.period.ms: 60000` **Integer** - the sample period in milliseconds

Parameters concerning fault tolerance:

- `dragon.faults.component.tolerance: 3` **Integer** - number of faults (exceptions caught) for any component after which the topology is halted 
- `dragon.faults.deadnone.timeout.ms: 600000` **Integer** - timeout in milliseconds before a dead node is contacted again to try and include it

Parameters concerning InfluxDB:

- `influxdb.url: ` **String** - the URL to use for the InfluxDB, if available. If this parameter is not given then InfluxDB will not be used.
- `influxdb.token: ` **String** the authorization token used to access the InfluxDB
- `influxdb.bucket: ` **String** the InfluxDB bucket to use for storing data samples
- `influxdb.organization: ` **String** the organization name for storing data samples

Parameters concerning topology mapping:

- `dragon.embedding.algorithm: dragon.topology.RoundRobinEmbedding` **String** - the embedding algorithm that maps a task in the topology to a host node

Other parameters:

- `dragon.data.dir: /tmp/dragon` **String** - the data directory where Dragon can store temporary files such as submitted jar files



# Network mode

Running in Network mode requires starting Dragon daemons on a number of hosts that are all visible to each other on the network. Multiple daemons can be started on a single host, but the service and data ports must be configured to be non-conflicting for all instances, i.e. each daemon must have its own `dragon.yaml` configuration file with `dragon.network.local.data.port` and `dragon.network.local.service.port` set appropriately. Command line options can be used to override the configuration parameters, if a single `dragon.yaml` is preferred. Each daemon is a single JVM.

To start a Dragon daemon, run:

    dragon -d

The daemon will attempt to make a connection to the first available other Dragon daemon listed in the `dragon.network.hosts` parameter. To override the host name, service and data port of the daemon being started run:

    dragon -d -h HOST_NAME -p DATA_PORT -s SERVICE_PORT
    
# Client commands

Client commands will timeout according to the `dragon.service.timeout.ms` parameter which defaults at 30 seconds. Timeouts may be for various reasons. Sometimes trying the command again will be met with success, e.g. if the timeout was due to a transient network failure.

- submit (inferred through the topology class itself)
- status `-st` or `--status`
    - list the dragon daemons, showing their current state and which topologies are running on them
- list `-l` or `--list`
    - list the topologies that are running on the dragon daemons, including all component instances and some statistics for them
- terminate `-X` or `--terminate`
    - stop spouts from emitting tuples, wait for all bolts to complete processing and remove the topology
- halt `-x` or `--halt`
    - put all threads for the topology into a waiting state
- resume `-r` or `--resume`
    - signal all threads for a topology to leave the waiting state
- purge `-P` or `--purge`
    - forcibly remove a topology immediately, without waiting for outstanding data to finish processing

All of the above commands can also be executed like:

    dragon -h HOST_NAME status
    
where the command becomes a positional argument.

The state of a topology on each daemon includes:

- `ALLOCATED` a transient state where the topology exists but is not yet ready for a start command 
- `SUBMITTED` a transient state where the topology is awaiting a start command
- `RUNNING` the topology is active
- `HALTED` the topology is inactive and can be resumed
- `TERMINATING` the topology is waiting for all outstanding tuples to be processed (spouts have been closed) after which the topology is deleted from memory and garbage collected
- `FAULT` the topology is not active and cannot be resumed, some data may be lost - the topology can only be purged and restarted.

Assuming there are no errors, a topology moves from `ALLOCATED` to `SUBMITTED` and then to `RUNNING` as quickly as possible. In the `RUNNING` state the topology can enter the `HALTED` state either because one or more topology components throws too many exceptions which triggers the transition to `HALTED` automatically, or because the topology receives a client command to halt. In the `HALTED` state the topology may be resumed, i.e. set back to `RUNNING` via a client command only. However if the topology continues to throw exceptions it will go back to `HALTED`. In either the `RUNNING` or `HALTED` states the topology may be terminated by a client command only.

A topology will enter the `FAULT` state if JVM processes die which including some part of the topology. As well, if the topology throws exceptions during transition between states, e.g. when going from `SUBMITTED` to `RUNNING` then it will be put into the `FAULT` state. A topology that has faulted can only be purged. It can then be restarted.

## Submitting a topology

Submitting a topology to a Dragon daemon requires providing the host name and optional service port of the daemon, as well as the topology JAR, topology class, and positional argument for the topology name:

    dragon -h HOST_NAME -s SERVICE_PORT -j YOUR_TOPOLOGY_JAR.jar -c YOUR.PACKAGE.TOPOLOGY TOPOLOGY_NAME

The topology JAR will be uploaded and stored at all Dragon daemons that the topology maps to, according to the embedding algorithm used (explained later). It will commence running immediately.

## Config settings

The Config settings on a daemon are first set using the defaults, then what is found in `dragon.yaml` and finally what is supplied by the submitted topology. 

## Getting the status of the Dragon daemons

To see the status of all Dragon daemons, use the status command:

    dragon -h HOST_NAME status

This should show something like:

	$./dragon.sh -h 192.168.1.1 status
	dragon version 0.0.1-beta-SNAPSHOT
	connecting to dragon daemon: [192.168.1.1:4001]
	requesting status...
	sending [GET_STATUS]
	waiting up to [30] seconds...
	received [STATUS]
	sending [SERVICE_DONE]
	 ── <dragon>
	    ├── [192.168.1.1:4001] (primary) OPERATIONAL at Thu Feb 27 19:41:10 SGT 2020
	    │   ├── partition: primary
	    │   ├── <context>
	    │   │   ├── 192.168.1.1:4001
	    │   │   ├── 192.168.1.2:4001
	    │   │   └── 192.168.1.3:4001
	    │   └── <topologies>
	    │       └── [yourtopo] RUNNING
	    ├── [192.168.1.2:4001] (primary) OPERATIONAL at Thu Feb 27 19:41:10 SGT 2020
	    │   ├── partition: primary
	    │   ├── <context>
	    │   │   ├── 192.168.1.1:4001
	    │   │   ├── 192.168.1.2:4001
	    │   │   └── 192.168.1.3:4001
	    │   └── <topologies>
	    │       └── [yourtopo] RUNNING


## Listing topology information

To list information about all topologies, including their state and any exceptions thrown with stack traces:

    dragon -h HOST_NAME list

This should show something like:

	$./dragon.sh -h 192.168.1.1 list
	dragon version 0.0.1-beta-SNAPSHOT
	connecting to dragon daemon: [192.168.1.1:4001]
	requesting list of topologies...
	sending [LIST_TOPOLOGIES]
	waiting up to [30] seconds...
	received [TOPOLOGY_LIST]
	sending [SERVICE_DONE]
	 ── <dragon>
	    └── [yourtopo]
	        ├── [192.168.1.1:4001] RUNNING
	        │   ├── [querySpout:0] emt:0,exe:0,trf:0
	        │   ├── [DBBolt:3] emt:0,exe:23546,trf:0
	        │   ├── [DBBolt:8] emt:0,exe:23543,trf:0
	        │   ├── [PrepBolt:1] emt:23546,exe:23546,trf:141276
	        │   ├── [PrepBolt:6] emt:23545,exe:23545,trf:141270
	        │   ├── [AnalysisBolt:1] emt:4768,exe:23548,trf:4768
	        │   └── [AnalysisBolt:6] emt:4656,exe:23545,trf:4656
	        ├── [192.168.1.2:4001] RUNNING
	        │   ├── [dataSpout:0] emt:235452,exe:0,trf:235452
	        │   ├── [DBBolt:4] emt:0,exe:23546,trf:0
	        │   ├── [DBBolt:9] emt:0,exe:23544,trf:0
	        │   ├── [PrepBolt:2] emt:23545,exe:23545,trf:141270
	        │   ├── [PrepBolt:7] emt:23545,exe:23545,trf:141270
	        │   ├── [AnalysisBolt:2] emt:4623,exe:23548,trf:4623
	        │   └── [AnalysisBolt:7] emt:4852,exe:23543,trf:4852

The values `emt`, `exe` and `trf` are emitted, executed and transferred resp. over the life time of the topology.


## Halting a topology

To halt a topology:

    dragon -h HOST_NAME -x -t TOPOLOGY_NAME

Halting the topology will suspend all of its threads and keep all data in place. It may be resumed later. A topology is also halted automatically if any of its components throw too many errors.

## Resume a topology

To resume a topology:

    dragon -h HOST_NAME -r -t TOPOLOGY_NAME

Resuming a topology that was halted due to errors will likely result in it being halted again in the near future, as soon as another error is thrown.

## Terminating a topology

To terminate a topology:

    dragon -h HOST_NAME -X -t TOPOLOGY_NAME
    
For a large topology over a number of nodes you may need to wait some time for it to terminate. This is because Dragon first *closes* all Spouts, then waits for all existing data to be fully processed (all outstanding messages to be communicated, which may lead to further processing, etc.) before proceeding to close all Bolts and release all of the resources that the topology consumed. If your topology is not "well behaved" it may not terminate. A well behaved topology will cease to emit tuples, and terminate any transient threads, when the `close` method has been called on Spouts and Bolts.

## Purge a topology

If a topology does not terminate, or it is in the `FAULT` state, then it must be purged to remove it:

    dragon -h HOST_NAME -P -t TOPOLOGY_NAME

## Configuring the embedding algorithm for a topology

There are two embedding algorithms available with dragon:
1. `dragon.topology.RoundRobinEmbedding` - embeds each task to connected daemons in a round robin manner (default algorithm)
2. `dragon.topology.FileBasedCustomEmbedding` - embeds each task to a connected daemon as defined via an external configuration file

The preferred algorithm can be configured via the `dragon.embedding.algorithm` configuration either programmatically in the topology:

    Config conf = new Config();
    conf.put(Config.DRAGON_EMBEDDING_ALGORITHM, "dragon.topology.FileBasedCustomEmbedding");

or in the `dragon.yaml` file:

    dragon.embedding.algorithm: dragon.topology.FileBasedCustomEmbedding

Further embedding algorithms can developed by implementing the `dragon.topology.IEmbeddingAlgo` interface.

### File based custom embedding algorithm

After enabling as mentioned above, `dragon.topology.FileBasedCustomEmbedding` requires an external YAML configuration file that maps a task into one or more hosts in a valid YAML file with the following format:

    "spout name or bolt name": ["node 1 host name:node 1 port", "node 2 host name:node 2 port",...]
For example:

    "numberSpout": ["localhost:4001"]
    "textSpout": ["localhost:4001","localhost:4101"]
    "shuffleBolt": ["localhost:4101"]
    "shuffleTextBolt": ["localhost:4101"]
    "numberBolt": ["localhost:9999","localhost:4101","localhost:4001"]

The file name can be configured programmatically in the topology:

    conf.put(Config.DRAGON_EMBEDDING_CUSTOM_FILE, "embedding.yaml");
    
or in the `dragon.yaml` file:

    dragon.embedding.custom.file: embedding.yaml
    
The `dragon.topology.FileBasedCustomEmbedding` algorithm will look for the configured file name, first in the current directory and then in the class path of the topology jar file.
The default embedding file name is `embedding.yaml`.

## Metrics Monitor

Metrics is available only in Network mode. A simple text based metrics monitor can be run:

    dragon dragon.MetricsMonitor -t TOPOLOGY_NAME

Note that the Metrics Monitor needs the `dragon.network.hosts` parameter to be set, that lists all Dragon hosts in the system.


