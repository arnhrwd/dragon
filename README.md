
# Dragon

A high performance, distributed stream processing system, loosely based on the Apache Storm API.

# Distribution usage

The Dragon distribution has the following directory structure and relevant files:

    dragon
    |   README.md
    └───bin
    |   |   dragon.sh
    |
    └───conf
    |   |   dragon.yaml
    |
    └───lib
        |   dragon.jar

To see simple help on using Dragon:

    cd dragon/bin
    ./dragon.sh -h
 
Detailed usage instructions are below.

# Compiling

To install Dragon into your local cache:

    mvn install
    
## Dependency

Include the dependency in your project's `pom.xml`: 

    <dependency>
        <groupId>au.edu.unimelb</groupId>
        <artifactId>dragon</artifactId>
        <version>0.0.1-SNAPSHOT</version>
        <scope>provided</scope>
    </dependency>
    
# Note about class loading in Java 11

Dynamic addition of a JAR file to the class path is used by Dragon to load a topology class, and all associated classes, from a supplied topology JAR file. The current practice, in Java 11, is to use the `instrumentation` package to achieve this, which requires a Java agent to load prior to the `main` method. Assuming `dragon.jar` is the JAR file with dependencies compiled in then running Dragon directly from the command line becomes:

    java -javaagent:dragon.jar -jar dragon.jar ...
 
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
- `../conf/dragon.yaml`
- `/etc/dragon/dragon.yaml`
- `${HOME}/.dragon/dragon.yaml`

The first file found will be used to load the parameters. If you wish to specify the configuration file name on the command line then use:

    dragon -C YOUR_CONF.yaml ...

## Parameters

The available parameters and their defaults are listed below.

### General parameters

- `dragon.output.buffer.size: 16` **Integer** - the size of the buffers on Spout and Bolt outputs
- `dragon.input.buffer.size: 16` **Integer** - the size of the buffers on Spout and Bolt inputs
- `dragon.base.dir: /tmp/dragon` **String** - the base directory where Dragon can store files such as submitted jar files and check point data
- `dragon.jar.dir: jars` **String** - the sub-directory to store jars files within
- `dragon.localcluster.threads: 2 ` **Integer** - the size of the thread pool that transfers tuples within a local cluster
- `dragon.embedding.algorithm: dragon.topology.RoundRobinEmbedding` **String** - the embedding algorithm that maps a task in the topology to a host node

### Network mode parameters

Buffer and thread resources:

- `dragon.router.input.threads: 1` **Integer** - the size of the thread pool that transfers tuples into the local cluster from the network (note that values larger than 1 result in tuple reordering on streams)
- `dragon.router.output.threads: 1` **Integer** - the size of the thread pool that transfers tuples out of the local cluster to the network (note that values large than 1 result in tuple reordering on streams)
- `dragon.router.input.buffer.size: 16` **Integer** - the size of the buffers for tuples transferring into the local cluster from the network
- `dragon.router.output.buffer.size: 16` **Integer** - the size of the buffers for tuples transferring out of the local cluster to the network

Comms details:

- `dragon.comms.retry.ms: 10000` **Integer** - the number of milliseconds to wait between retries when attempting to make a connection
- `dragon.comms.retry.attempts: 30` **Integer** - the number of retries to make before suspending retry attempts
- `dragon.comms.reset.count: 128` **Integer** - (advanced) the number of network tasks transmitted over object stream before reseting the object stream handle table
- `dragon.comms.incoming.buffer.size: 1024` **Integer** - the size of the buffer for incoming network tasks, shared over all sockets

Network details:

- `dragon.network.default.service.port: 4000` **Integer** - the default service port
- `dragon.network.default.data.port: 4001` **Integer** - the default data port
- `dragon.network.local.host: localhost` **String** - the default advertised host name for the Dragon daemon
- `dragon.network.local.service.port: ` **Integer** - the service port for the Dragon daemon, if not set then the default service port is used 
- `dragon.network.local.data.port: ` **Integer** - the data port for the Dragon daemon, if not set then the default data port is used
- `dragon.network.hosts: [{hostname:[port,port]},{hostname:[port,port]}]` **HostArray** - strictly an array of dictionaries, where the sole dictionary key in each array element is the host name of a Dragon daemon and its associated array is the service and data ports in that order, all Dragon daemons should be listed including the local one

The `dragon.network.hosts` parameter can also be written like this:

    dragon.network.remote.hosts:
    - hostname: [port,port]
    - hostname: [port,port]
    
If only one port is given, i.e. `[port]` then it is assumed to be the service port (the data port will be the default value), and if no ports are given (in which case write `[]`) then both ports take their default values.

Parameters concerning metrics:

- `dragon.metrics.enable: true` **Boolean** - whether the Dragon daemon should record metrics
- `dragon.metrics.sample.history: 1` **Integer** - how much sample history to record
- `dragon.metrics.sample.period.ms: 60000` **Integer** - the sample period in milliseconds

Parameters concerning object recycling (advanced):

- `dragon.recycler.tuple.capacity: 1024` **Integer** - number of tuple objects to allocate in advance
- `dragon.recycler.tuple.expansion: 1024` **Integer** - number of tuple objects to increase the tuple pool by when/if the tuple pool capacity is reached
- `dragon.recycler.tuple.compact: 0.2` **Double** - fraction of capacity the tuple pool size must reach to trigger compaction of the pool
- `dragon.recycler.task.capacity: 1024` **Integer** - number of network task objects to allocate in advance
- `dragon.recycler.task.expansion: 1024` **Integer** - number of network task objects to increase the network task pool by when/if the tuple pool capacity is reached
- `dragon.recycler.task.compact: 0.2` **Double** - fraction of capacity the network task pool size must reach to trigger compaction of the pool


# Network mode

Running in Network mode requires starting Dragon daemons on a number of hosts that are all visible to each other on the network. Multiple daemons can be started on a single host, but the service and data ports must be configured to be non-conflicting for all instances, i.e. each daemon must have its own `dragon.yaml` configuration file with `dragon.network.local.data.port` and `dragon.network.local.service.port` set appropriately. Command line options can be used to override the configuration parameters, if a single `dragon.yaml` is preferred. Each daemon is a single JVM.

To start a Dragon daemon, run:

    dragon -d

The daemon will attempt to make a connection to the first available other Dragon daemon listed in the `dragon.network.hosts` parameter. To override the host name, service and data port of the daemon being started run:

    dragon -d -h HOST_NAME -p DATA_PORT -s SERVICE_PORT

## Submitting a topology

Submitting a topology to a Dragon daemon requires providing the host name and optional service port of the daemon, as well as the topology JAR, topology class, and positional argument for the topology name:

    dragon -h HOST_NAME -s SERVICE_PORT -j YOUR_TOPOLOGY_JAR.jar -c YOUR.PACKAGE.TOPOLOGY TOPOLOGY_NAME

The topology JAR will be uploaded and stored at all Dragon daemons that the topology maps to, according to the embedding algorithm used (explained later). It will commence running immediately.

## Config settings

The Config settings on a daemon are first set using the defaults, then what is found in `dragon.yaml` and finally what is supplied by the submitted topology. 

## Terminating a topology

To terminate a topology:

    dragon -h HOST_NAME -s SERVICE_PORT -x -t TOPOLOGY_NAME
    
For a large topology over a number of nodes you may need to wait some time for it to terminate. This is because Dragon first *closes* all Spouts, then waits for all existing data to be fully processed (all outstanding messages to be communicated, which may lead to further processing, etc.) before proceeding to close all Bolts and release all of the resources that the topology consumed. If your topology is not "well behaved" it may not terminate. A well behaved topology will cease to emit tuples, and terminate any transient threads, when the `close` method has been called on Spouts and Bolts.

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

# Porting from an Apache Storm Project

The Dragon API is loosely based on Version 0.10 of Apache Storm. E.g. package names starting with `backtype.storm` can be replaced with `dragon`. There are some changes and stipulations:

- `implements CustomStreamGrouping` becomes `extends AbstractGrouping`
- all topology objects **must** be serializable, as the entire topology is serialized when submitting it

