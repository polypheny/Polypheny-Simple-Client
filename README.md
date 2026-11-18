# Polypheny Simple Client

A simple benchmarking and testing client for Polypheny-DB. It includes support for the following benchmarks:

* **Gavel**: The Gavel benchmark simulates the workload of a fictional auction house.
* **kNN-Bench**: A benchmark tailored towards typical workloads of multimedia retrieval applications and especially k-Nearest-Neighbor search.
* **Multimedia**: This benchmarks produces workload containing or requesting randomly generated BLOBs.
* **Graph**: A simple synthetic graph benchmark that executes a mixture of DQL and DML Cypher queries against a pseudo-random network.
* **DocBench**: A simple benchmark for document schemas generating MongoQL queries.
* **MultiBench**: A benchmark that combines Gavel, Graph, and DocBench.
* [**AuctionMark**](http://hstore.cs.brown.edu/projects/auctionmark/): A benchmark executing workload that simulates the activities found in a well-known auction site.
* [**SmallBank**](http://ses.library.usyd.edu.au/bitstream/2123/5353/1/michael-cahill-2009-thesis.pdf): The SmallBank benchmark models a simple banking application.
* [**TPC-C**](http://www.tpc.org/tpcc/): A well-known on-line transaction processing (OLTP) benchmark.
* [**TPC-H**](http://www.tpc.org/tpch/): A decision support benchmark consisting of a suite of business oriented ad-hoc queries and concurrent data modifications.
* [**YCSB**](https://github.com/brianfrankcooper/YCSB): Benchmark for evaluating the performance of "key-value" and "cloud" serving stores.
* [**MMSBench-Net**](https://gvdb23.informatik.uni-stuttgart.de/wp-content/uploads/2023/06/GvDB2023_Lengweiler.pdf) A benchmark for multi-model database systems simulating a network monitoring application.

This benchmarking client contains a [Chronos](https://chronos-eaas.org/) connector. This allows to easily execute evaluation campaigns.

## Getting Started

This client can either be started in a stand-alone mode for quick testing or in a chronos mode for automated benchmarking using [Chronos](https://chronos-eaas.org/).

### Chronos

```
SYNOPSIS
        polypheny-simple-client.jar chronos [ {-cc | --chronos} <chronos> ]
                [ {-e | --environment} <environment> ] [ --host <IP or Port> ] 
                [ --control-username <Polypheny Control Username> ] 
                [ --control-password <Polypheny Control Password> ] 
                [ {-p | --port} <port> ] [ --queryList ] {-s | --supports} <supports> 
                [ --writeCSV ]

OPTIONS
        -cc <chronos>, --chronos <chronos>
            Hostname or IP address of the Chronos Control.

        -e <environment>, --environment <environment>
            Identifier of the Chronos evaluation environment this client
            belongs to (default: "default").

        --host <IP or Port>
            Hostname or IP address of the host running the system(s) to be
            benchmarked (default: 127.0.0.1).
            
        --control-username <Polypheny Control Username>
            The username to be used for authentication with Polypheny Control.
            
        --control-password <Polypheny Control Password>
            The password to be used for authentication with Polypheny Control.

        -p <port>, --port <port>
            Port of the REST API of the Chronos Control server (default: 443).

        --queryList
            Dump all Gavel queries as SQL into a file (default: false).

        -s <supports>, --supports <supports>
            Comma-separated list of system identifiers supported by this
            client. Depends on the Chronos instance.

        --writeCSV
            Write a CSV file containing execution times for all executed
            queries (default: false).
```

### Stand-alone

This client can be used by specifying the name of the benchmark as first parameter and the task as second parameter. Optionally, it is also possible to specify a multiplier for the data and workload (integer > 0). The general syntax is identical for all benchmarks:

```
java -jar polypheny-simple-client.jar  BENCHMARK  TASK  [ MULTIPLIER ] 
```

Use the `help` command to get an overview of all available parameters for a benchmark. For example:

```
java -jar polypheny-simple-client.jar help gavel  
```

_BENCHMARK_: `{ gavel | knnbench | multimedia | graph | docbench | multibench | auctionmark | smallbank | tpcc | tpch | ycsb | coms }`

_TASK_: `{ schema | data | workload | warmup }`

_MULTIPLIER_: Optional parameter taking an integer for multiplying the number of data and queries. Defaults to 1.

## Roadmap

See the [open issues](https://github.com/polypheny/Polypheny-DB/labels/A-client) for a list of proposed features (and known issues).

## Contributing

We highly welcome your contributions to _Polypheny Simple Client_. If you would like to contribute, please fork the repository and submit your changes as a pull request. Please consult our [Admin Repository](https://github.com/polypheny/Admin) and our [Website](https://polypheny.org) for guidelines and additional information.

Please note that we have a [code of conduct](https://github.com/polypheny/Admin/blob/master/CODE_OF_CONDUCT.md). Please follow it in all your interactions with the project.


## Credits

_Polypheny Simple Client_ builds upon the great work of several other open-source projects:

* [Airline](https://rvesse.github.io/airline/): Annotation-driven Java library for building command line interfaces.
* [Apache Commons](http://commons.apache.org/): A bunch of useful Java utility classes.
* [Chronos](https://chronos-eaas.org/): A system for defining, executing, and monitoring evaluations.
* [jCodec](https://github.com/Devskiller/jfairy/): Pure java implementation of video/audio codecs.
* [jFairy](https://github.com/Devskiller/jfairy/): A library for generating fake data.
* [Log4j](https://logging.apache.org/log4j/2.x/): Fast and flexible logging framework for Java.
* [Opencsv](http://opencsv.sourceforge.net/): Library for parsing CSV (comma-separated values) data.
* [Project Lombok](https://projectlombok.org/): A library providing compiler annotations for tedious tasks.
* [Shadow](https://imperceptiblethoughts.com/shadow/): Gradle plugin for creating fat JARs.
* [SLF4J](http://www.slf4j.org/): Provides a logging API by means of a facade pattern.
* [Unirest](http://kong.github.io/unirest-java/): A lightweight HTTP client library.
* [zip4j](https://github.com/srikanth-lingala/zip4j): A Java library for zip files and streams.


## License

The MIT License (MIT)

### Chronos runner for CH2 and YCSB

With JDK 21, build the deployment package from the Simple Client checkout.
The default database artifact comes from the sibling Polypheny-DB checkout.

```sh
(cd ../Polypheny-DB && ./gradlew :dbms:shadowJar)
./gradlew chronosRunnerBundle
```

Use `-PpolyphenyDbJar=/path/to/dbms.jar` to package an existing database build.
For a Chronos server that requires the compatible client and job parser, pass
`-PchronosCompatibilityJar=/path/to/working-runner.jar`. Use a runner verified
against that server. The bundle build imports its `ChronosHttpClient` and
`ChronosJob` classes, including their nested classes. The default uses the
pinned published Chronos agent 2.5.1.

Extract the ZIP from `build/distributions` on a host with Docker and JDK 21.
Run from the extracted directory. Configure the system identifiers registered
on your Chronos server once in the runner environment.

```sh
export CHRONOS_SUPPORTS='system-identifier'
java -jar simpleclient.jar chronos --chronos https://chronos.example --environment default
```

The agent polls for matching jobs. Use `--job ID` for a single run.
Use the CDL examples in `docker/ch2/chronos-job.json` and
`docker/ycsb/chronos-job.json` when defining runs on your server.
The agent builds the Docker images from the bundled context and Docker reuses
unchanged layers. Each run creates its own network and containers and archives
results and logs under `results/chronos-ch2` or `results/chronos-ycsb`.

The local defaults are in each benchmark's `experiment.properties`. Override
the file with `--ch2-config` or `--ycsb-config`. When moving a configuration
file, set `chronos.buildContext` to the extracted root, using an absolute path
or a path relative to the configuration file. Put image overrides in the local
properties or the job CDL according to the examples. CH2 reports committed
document counts in `polypheny/load/load.log` during data loading. The agent
logs the absolute path when it starts the container, so you can follow it
with `tail -f`.

### Run CH2 or YCSB locally with Docker

Build and extract the runner bundle using the commands above. Run these
commands from its root with Docker running and JDK 21 installed.

```sh
java -jar simpleclient.jar ycsb-external all --managed --output results/ycsb-local
java -jar simpleclient.jar ch2-external all --managed --output results/ch2-local
```

Managed runs build the images and start a fresh MongoDB-backed Polypheny
instance. They run setup, load, optional warmup and workload, then remove the
job's containers and network. Logs and results stay in the output directory.
Use a new output directory for each run. Managed YCSB runs the Polypheny target.

For an existing environment, omit `--managed`. The tasks are `setup` (alias
`schema`), `data`, `warmup`, `workload` and `all`. Use `--config` to select
an experiment properties file.
