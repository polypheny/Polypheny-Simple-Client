package org.polypheny.simpleclient.main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.polypheny.simpleclient.scenario.external.ExternalConfig;

final class ExternalDockerEnvironment implements AutoCloseable {
    final String id;
    private final String benchmark;
    final String network;
    private final String mongo;
    private final String polypheny;
    private final Path output;
    private boolean networkAttempted;
    private final List<String> containers = new ArrayList<>();

    ExternalDockerEnvironment( String benchmark, Path output ) {
        this.benchmark = benchmark;
        id = "chronos-" + benchmark + "-" + UUID.randomUUID();
        network = id + "-net";
        mongo = id + "-mongo";
        polypheny = id + "-polypheny";
        this.output = output;
    }

    void startMongoBackedPolypheny( ExternalConfig config ) throws IOException, InterruptedException {
        buildImage( config.buildContext, benchmark, config.imageName, config.buildTimeoutSeconds );
        buildImage( config.buildContext, "polypheny", config.polyphenyImage, config.buildTimeoutSeconds );
        createNetwork();
        startMongo( config.mongoImage, config.buildTimeoutSeconds, config.startupTimeoutSeconds );
        startPolypheny( config.polyphenyImage );
    }

    private void createNetwork() throws IOException, InterruptedException {
        networkAttempted = true;
        runDocker( List.of( "network", "create", "--label", "chronos." + benchmark + "=" + id, network ) );
    }

    private void startPolypheny( String image ) throws IOException, InterruptedException {
        containers.addFirst( polypheny );
        runDocker( List.of( "run", "-d", "--pull=never", "--name", polypheny, "--label", "chronos." + benchmark + "=" + id,
                "--network", network, "--network-alias", "polypheny", image ) );
    }

    private void startMongo( String image, int pullTimeout, int startupTimeout ) throws IOException, InterruptedException {
        Path log = output.resolve( "docker.log" );
        if ( DockerLauncher.dockerCommand( List.of( "image", "inspect", image ), log.toFile(), 30 ) != 0 ) {
            runDocker( List.of( "pull", image ), log, pullTimeout );
        }

        containers.add( mongo );
        runDocker( List.of( "run", "-d", "--pull=never", "--name", mongo, "--label", "chronos." + benchmark + "=" + id,
                "--network", network, "--network-alias", "mongo", image,
                "--replSet", benchmark, "--bind_ip_all" ) );
        long deadline = System.nanoTime() + startupTimeout * 1_000_000_000L;
        awaitMongo( "quit(db.adminCommand({ping:1}).ok === 1 ? 0 : 1)", deadline );
        runDocker( List.of( "exec", mongo, "mongosh", "--quiet", "--eval",
                "const r=rs.initiate({_id:'" + benchmark + "',members:[{_id:0,host:'mongo:27017'}]}); quit(r.ok === 1 ? 0 : 1)" ) );
        awaitMongo( "quit(db.hello().isWritablePrimary ? 0 : 1)", deadline );
    }

    void awaitPolypheny( String image, List<String> command, Map<String, String> environment, File input, int timeout )
            throws IOException, InterruptedException {
        long deadline = System.nanoTime() + timeout * 1_000_000_000L;
        RuntimeException lastFailure = null;
        while ( System.nanoTime() < deadline ) {
            try {
                DockerLauncher.runJob( id + "-probe-" + UUID.randomUUID(), image, environment, network,
                        command, output.resolve( "polypheny-readiness.log" ).toFile(), Map.of(), input, 15 );
                return;
            } catch ( RuntimeException e ) {
                if ( Thread.currentThread().isInterrupted() ) {
                    throw e;
                }
                lastFailure = e;
                Thread.sleep( 500 );
            }
        }
        throw new IOException( "Polypheny did not become ready; see " + output, lastFailure );
    }

    private void buildImage( Path context, String name, String image, int timeout ) throws IOException, InterruptedException {
        Path dockerfile = context.resolve( "docker/" + name + "/Dockerfile" );
        if ( !Files.isRegularFile( dockerfile ) ) {
            throw new IOException( "Missing " + dockerfile + "; check chronos.buildContext in experiment.properties" );
        }
        // Docker reuses unchanged layers, including benchmark downloads.
        runDocker( List.of( "build", "-f", dockerfile.toString(), "-t", image, context.toString() ),
                output.resolve( "build-" + name + ".log" ), timeout );
    }

    private void awaitMongo( String script, long deadline ) throws IOException, InterruptedException {
        while ( System.nanoTime() < deadline ) {
            if ( DockerLauncher.dockerCommand( List.of( "exec", mongo, "mongosh", "--quiet", "--eval", script ),
                    output.resolve( "mongo-readiness.log" ).toFile(), 10 ) == 0 ) {
                return;
            }
            Thread.sleep( 500 );
        }
        throw new IOException( "MongoDB did not become ready; see " + output );
    }

    private void runDocker( List<String> command ) throws IOException, InterruptedException {
        runDocker( command, output.resolve( "docker.log" ), 30 );
    }

    private void runDocker( List<String> command, Path log, int timeout ) throws IOException, InterruptedException {
        if ( DockerLauncher.dockerCommand( command, log.toFile(), timeout ) != 0 ) {
            throw new IOException( "Docker " + String.join( " ", command ) + " failed; see " + log );
        }
    }

    @Override
    public void close() throws IOException {
        // Finish cleanup even if the job was cancelled, then restore the interrupt flag.
        boolean interrupted = Thread.interrupted();
        IOException failure = new IOException( "Could not clean benchmark resources; see " + output );
        try {
            for ( String container : containers ) {
                interrupted |= cleanup( List.of( "logs", container ), output.resolve( container + ".log" ), 15, failure );
                interrupted |= cleanup( List.of( "rm", "-f", "-v", container ), output.resolve( "docker.log" ), 30, failure );
            }
            if ( networkAttempted ) {
                interrupted |= cleanup( List.of( "network", "rm", network ), output.resolve( "docker.log" ), 30, failure );
            }
        } finally {
            if ( interrupted ) {
                Thread.currentThread().interrupt();
            }
        }
        if ( failure.getSuppressed().length > 0 ) {
            throw failure;
        }
    }
    private boolean cleanup( List<String> command, Path log, int timeout, IOException failure ) {
        try {
            // A container may have failed to start and have no logs.
            if ( command.getFirst().equals( "logs" ) ) {
                DockerLauncher.dockerCommand( command, log.toFile(), timeout );
            } else {
                runDocker( command, log, timeout );
            }
        } catch ( IOException | InterruptedException e ) {
            failure.addSuppressed( e );
            return e instanceof InterruptedException;
        }
        return false;
    }


}
