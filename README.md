
# Dragon

A high performance stream processing system, loosely based on the Apache Storm API.

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

# Local mode

Assuming `dragon.jar` is the jar file with dependencies compiled in:

    java -jar dragon.jar -h

will provide help on options. To execute a topology in local mode:

    java -jar dragon.jar -j YOUR_TOPOLOGY_JAR.jar -c YOUR.PACKAGE.TOPOLOGY

Running in local mode, Dragon creates a *local cluster* in a single JVM that contains all Spouts and Bolts without any need for networking.

# Configuration

Dragon looks for `dragon.properties` in several places, in this order:

- `./dragon.properties`
- `../conf/dragon.properties`
- `/etc/dragon/dragon.properties`
- `${HOME}/.dragon/dragon.properties`

The first file found will be used to load the properties.

## Properties

The available properties and their defaults are listed below.

Parameters that affect both local and remotely submitted topologies:

- `dragon.output.buffer.size = 1024` **Integer** - the size of the buffers on Spout and Bolt outputs
- `dragon.input.buffer.size = 1024` **Integer** - the size of the buffers on Spout and Bolt inputs
- `dragon.base.dir = /tmp/dragon` **String** - the base directory where Dragon can store files such as submitted jar files and check point data
- `dragon.jar.dir = jars` **String** - the sub-directory to store jars files within
- `dragon.localcluster.threads = 10` **Integer** - the size of the thread pool that transfers tuples within a local cluster
- `dragon.embedding.algorithm = dragon.topology.RoundRobinEmbedding` **String** - the embedding algorithm that maps a task in the topology to a host node

Parameters that affect only remotely submitted topologies:

- `dragon.router.input.threads = 10` **Integer** - the size of the thread pool that transfers tuples into the local cluster from the network
- `dragon.router.output.threads = 10` **Integer** - the size of the thread pool that transfers tuples out of the local cluster to the network
- `dragon.router.input.buffer.size = 1024` **Integer** - the size of the buffers for tuples transferring into the local cluster from the network
- `dragon.router.output.buffer.size = 1024` **Integer** - the size of the buffers for tuples transferring out of the local cluster to the network
- `dragon.network.remote.host =` **String** - the name of the remote host to connect to, for subsequent Dragon nodes (do not send this value for the initial Dragon node)
- `dragon.network.remote.service.port = 4000` **Integer** - the port number used by the remote host for receiving service messages
- `dragon.network.remote.node.port = 4001` **Integer** - the port number used by the remote host for receiving data messages
- `dragon.network.local.host = localhost` **String** - the name used to advertise the local Dragon node
- `dragon.network.local.service.port = 4000` **Integer** - the port number used by the local Dragon node for receiving service messages
- `dragon.network.local.node.port = 4001` **Integer** - the port number used by the local Dragon node for receiving node messages
- `dragon.network.remote.hosts =` **String** - comma separate list of all Dragon nodes in the network, providing port numbers for both service and node ports, in the form: `host:port:port,host:port:port,...`

Parameters concerning metrics:

- `dragon.metrics.enable = true` **Boolean** - whether the node should record metrics
- `dragon.metrics.sample.history = 1` **Integer** - how much sample history to record
- `dragon.metrics.sample.period.ms = 60000` **Integer** - the sample period in milliseconds

# Cluster mode

Running in cluster mode requires starting an initial Dragon node, and then starting further Dragon nodes that connect to the initial Dragon node, or any existing Dragon nodes. The Dragon nodes will connect to form a fully connected network. Therefore they must all be visible to each other on the network.

To start an initial Dragon node, ensure that `dragon.network.remote.host` is **not** set in `dragon.properties`, and make sure that `dragon.network.local.host` is set to be the IP address or domain name of the Dragon node, and run:

    java -jar dragon.jar -d

To start further nodes that connect to an existing Dragon node:

    java -jar dragon.jar -d -h REMOTE_HOST -p REMOTE_SERVICE_PORT

or set `dragon.network.remote.host = REMOTE_HOST` and run:

    java -jar dragon.jar -d

## Submitting a topology

Either:

    java -jar dragon.jar -h REMOTE_HOST -p REMOTE_SERVICE_PORT -j YOUR_TOPOLOGY_JAR.jar -c YOUR.PACKAGE.TOPOLOGY TOPOLOGY_NAME

or set `dragon.network.remote.host = REMOTE_HOST` and run:

    java -jar dragon.jar -j YOUR_TOPOLOGY_JAR.jar -c YOUR.PACKAGE.TOPOLOGY TOPOLOGY_NAME

## Terminating a topology

Either:

    java -jar dragon.jar -h REMOTE_HOST -p REMOTE_SERVICE_PORT -x -t TOPOLOGY_NAME
    
or set `dragon.network.remote.host = REMOTE_HOST` and run:

    java -jar dragon.jar -x -t TOPOLOGY_NAME
    
For a large topology over a number of nodes you may need to wait some time for it to terminate.

## Configuring the embedding algorithm for a topology

There are two embedding algorithms available with dragon:
1. `dragon.topology.RoundRobinEmbedding` - embeds each task to connected nodes in a round robin manner (default algorithm)
2. `dragon.topology.FileBasedCustomEmbedding` - embed each task to a connected node as defined via an external configuration file

The preferred algorithm can be configured via the `dragon.embedding.algorithm` configuration either programatically in the topology:

    Config conf = new Config();
    conf.put(Config.DRAGON_EMBEDDING_ALGORITHM, "dragon.topology.FileBasedCustomEmbedding");

or in the `dragon.properties` file:

    dragon.embedding.algorithm=dragon.topology.FileBasedCustomEmbedding

Further embedding algorithms can developed by implementing the `dragon.topology.IEmbeddingAlgo` interface.

### File based custom embedding algorithm

After enabling as mentioned above, `dragon.topology.FileBasedCustomEmbedding` requires an external YAML configuration file that maps a task into one or more host nodes in a valid YAML file with the following format:

    "spout name or bolt name": ["node 1 host name:node 1 port", "node 2 host name:node 2 port",...]
eg:

    "numberSpout": ["localhost:4001"]
    "textSpout": ["localhost:4001","localhost:4101"]
    "shuffleBolt": ["localhost:4101"]
    "shuffleTextBolt": ["localhost:4101"]
    "numberBolt": ["localhost:9999","localhost:4101","localhost:4001"]

The file name can be configured programmatically in the topology:

    conf.put(Config.DRAGON_EMBEDDING_CUSTOM_FILE, "embedding.yaml");
    
or in the `dragon.properties` file:

    dragon.embedding.custom.file=embedding.yaml
    
`dragon.topology.FileBasedCustomEmbedding` will look for the configured file name, first in the current directory and then in the class path of the topology jar file.
The default embedding file name is `embedding.yaml`.

## Metrics Monitor

Metrics is available only in Cluster mode. A simple text based metrics monitor can be run:

    java -cp dragon.jar dragon.MetricsMonitor -t TOPOLOGY_NAME

Note that the Metrics Monitor needs the `dragon.network.remote.hosts` parameter to be set, that lists all Dragon hosts in the system.

# Porting from an Apache Storm Project

The Dragon API is loosely based on Version 0.10 of Apache Storm. E.g. package names starting with `backtype.storm` can be replaced with `dragon`. There are some changes and stipulations:

- `implements CustomStreamGrouping` becomes `extends AbstractGrouping`
- all topology objects **must** be serializable, as the entire topology is serialized when submitting it

