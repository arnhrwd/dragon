
# Dragon

A high performance, distributed stream processing system, loosely based on the Apache Storm API.

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
    - hostname: 45.113.235.125
    - hostname: 45.113.235.146
    - hostname: 45.113.235.116
    dragon.deploy.dir: /home/ubuntu/packages

Dragon has a number of deployment commands available, with `deploy` bundling a number of them together. It takes the original distro package as a command line parameter.

    ./dragon.sh deploy PATH_TO_DISTRO/dragon-VERSION-distro.zip [USERNAME]

The distro must be one of `-distro.zip`, `-distro.tar.gz` or `-distro.tar.bz2`. The `USERNAME` is an optional parameter that is the username to login to the Ubuntu machines as, otherwise the user's login name will be used. The `deploy` command will install software on each Ubuntu machine:

    mkdir -p /home/ubuntu/packages && sudo apt update && sudo apt install -y openjdk-11-jre-headless unzip && sudo apt autoremove

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

Now you can submit a topology to the Dragon cluster. Other commands that are useful for controlling deployment are:

    ./dragon.sh offline [USERNAME]
    ./dragon.sh online [USERNAME]
    ./dragon.sh config [USERNAME]
    
These allow you to kill all Dragon daemons (bring them offline), bring them back online and to redo their configuration file, e.g. if you have added more Ubuntu machines, or parameters, etc.

# Detailed Usage
 
Detailed usage instructions are below.

# Compiling

To install Dragon into your local cache:

    mvn install
    
## Dependency

Include the dependency in your project's `pom.xml`, where `VERSION` is replaced with whatever version you are using: 

    <dependency>
        <groupId>au.edu.unimelb</groupId>
        <artifactId>dragon</artifactId>
        <version>VERSION</version>
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
- `dragon.data.dir: /tmp/dragon` **String** - the data directory where Dragon can store files such as submitted jar files and check point data
- `dragon.jar.dir: jars` **String** - the sub-directory to store jars files within
- `dragon.localcluster.threads: 2 ` **Integer** - the size of the thread pool that transfers tuples within a local cluster
- `dragon.embedding.algorithm: dragon.topology.RoundRobinEmbedding` **String** - the embedding algorithm that maps a task in the topology to a host node
- `dragon.java.bin: /usr/bin/java` **String** - the path of the Java binary

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

Primary daemon and network partition:

- `dragon.network.primary: true` **Boolean** - Dragon daemons listed in the `dragon.yaml` must all be listed as `true` (in fact, they will be set true anyway). A primary daemon is one which was set online by the command line interface. Non-primary daemons are created by primary daemons, when allocating new partitions and so on, as explained further in.
- `dragon.network.partition: primary` **String** - the partition name for this daemon

The primary daemon is responsible for starting up other daemons on the same machine, and monitoring them for liveness, restarting as needed. The primary daemon would usually be started via a remote `ssh` command. It is not necessary to use `supervisord` since the daemons will supervise each other.

Every Dragon daemon is part of a single partition, by default called the `primary` partition. When a topology is submitted, the embedding strategy can select the partition that the topology will be embedded into, which allows for complete process isolation of topologies.

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

Parameters concerning fault tolerance:

- `dragon.faults.component.tolerance: 3` **Integer** - number of faults (exceptions caught) for any component after which the topology is halted 

Parameters concerning InfluxDB:

- `influxdb.url: ` **String** - the URL to use for the InfluxDB, if available. If this parameter is not given then InfluxDB will not be used.
- `influxdb.token: ` **String** the authorization token used to access the InfluxDB
- `influxdb.bucket: ` **String** the InfluxDB bucket to use for storing data samples
- `influxdb.organization: ` **String** the organization name for storing data samples

# Network mode

Running in Network mode requires starting Dragon daemons on a number of hosts that are all visible to each other on the network. Multiple daemons can be started on a single host, but the service and data ports must be configured to be non-conflicting for all instances, i.e. each daemon must have its own `dragon.yaml` configuration file with `dragon.network.local.data.port` and `dragon.network.local.service.port` set appropriately. Command line options can be used to override the configuration parameters, if a single `dragon.yaml` is preferred. Each daemon is a single JVM.

To start a Dragon daemon, run:

    dragon -d

The daemon will attempt to make a connection to the first available other Dragon daemon listed in the `dragon.network.hosts` parameter. To override the host name, service and data port of the daemon being started run:

    dragon -d -h HOST_NAME -p DATA_PORT -s SERVICE_PORT
    
# Client commands

- submit (inferred through the topology class itself)
- list `-l` or `--list`
- terminate `-X` or `--terminate`
- halt `-x` or `--halt`
- resume `-r` or `--resume`

The state of a topology on each daemon includes:

- `ALLOCATED` a transient state where the topology exists but is not yet ready for a start command 
- `SUBMITTED` a transient state where the topology is awaiting a start command
- `RUNNING` the topology is active
- `HALTED` the topology is inactive and can be resumed
- `TERMINATING` the topology is waiting for all outstanding tuples to be processed (spouts have been closed) after which the topology is deleted from memory and garbage collected

Assuming there are no errors, a topology moves from `ALLOCATED` to `SUBMITTED` and then to `RUNNING` as quickly as possible. In the `RUNNING` state the topology can enter the `HALTED` state either because one or more topology components throws too many exceptions which triggers the transition to `HALTED` automatically, or because the topology receives a client command to halt. In the `HALTED` state the topology may be resumed, i.e. set back to `RUNNING` via a client command only. However if the topology continues to throw exceptions it will go back to `HALTED`. In either the `RUNNING` or `HALTED` states the topology may be terminated by a client command only.

## Submitting a topology

Submitting a topology to a Dragon daemon requires providing the host name and optional service port of the daemon, as well as the topology JAR, topology class, and positional argument for the topology name:

    dragon -h HOST_NAME -s SERVICE_PORT -j YOUR_TOPOLOGY_JAR.jar -c YOUR.PACKAGE.TOPOLOGY TOPOLOGY_NAME

The topology JAR will be uploaded and stored at all Dragon daemons that the topology maps to, according to the embedding algorithm used (explained later). It will commence running immediately.

## Config settings

The Config settings on a daemon are first set using the defaults, then what is found in `dragon.yaml` and finally what is supplied by the submitted topology. 

## Listing topology information

To list information about all topologies, including their state and any exceptions thrown with stack traces:

    dragon -h HOST_NAME -s SERVICE_PORT -l

## Halting a topology

To halt a topology:

    dragon -h HOST_NAME -s SERVICE_PORT -x -t TOPOLOGY_NAME

Halting the topology will suspend all of its threads and keep all data in place. It may be resumed later. A topology is also halted automatically if any of its components throw too many errors.

## Resume a topology

To resume a topology:

    dragon -h HOST_NAME -s SERVICE_PORT -r -t TOPOLOGY_NAME

Resuming a topology that was halted due to errors will likely result in it being halted again in the near future, as soon as another error is thrown.

## Terminating a topology

To terminate a topology:

    dragon -h HOST_NAME -s SERVICE_PORT -X -t TOPOLOGY_NAME
    
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

