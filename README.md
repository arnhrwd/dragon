
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

Parameters that affect only remotely submitted topologies:

- `dragon.router.input.threads = 10` **Integer** - the size of the thread pool that transfers tuples into the local cluster from the network
- `dragon.router.output.threads = 10` **Integer** - the size of the thread pool that transfers tuples out of the local cluster to the network
- `dragon.router.input.buffer.size = 1024` **Integer** - the size of the buffers for tuples transferring into the local cluster from the network
- `dragon.router.output.buffer.size = 1024` **Integer** - the size of the buffers for tuples transferring out of the local cluster to the network
- `dragon.network.remote.host =` **String** - the name of the remote host to connect to, for subsequent Dragon nodes (do not send this value for the initial Dragon node)
- `dragon.network.remote.service.port = 4000` **Integer** - the port number used by the remote host for receiving service messages
- `dragon.network.remote.node.port = 4001` **Integer** - the port number used by the remote host for receiving node messages
- `dragon.network.remote.task.port = 4002` **Integer** - the port number used by the remote host for receiving networking task messages
- `dragon.network.local.service.port = 4000` **Integer** - the port number used by the local Dragon node for receiving service messages
- `dragon.network.local.node.port = 4001` **Integer** - the port number used by the local Dragon node for receiving node messages
- `dragon.network.local.task.port = 4002` **Integer** - the port number used by the local Dragon node for receiving task messages

# Cluster mode

Running in cluster mode requires starting an initial Dragon node, and then starting further Dragon nodes that connect to the initial Dragon node, or any existing Dragon nodes. The Dragon nodes will connect to form a fully connected network. Therefore they must all be visible to each other on the network.

To start an initial Dragon node, ensure that `dragon.network.remote.host` is **not** set in `dragon.properties` and run:

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
