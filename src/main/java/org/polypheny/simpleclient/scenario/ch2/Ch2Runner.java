package org.polypheny.simpleclient.scenario.ch2;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.polypheny.simpleclient.main.DockerLauncher;
import org.polypheny.simpleclient.scenario.external.ExternalBenchmarkRunner;


public class Ch2Runner extends ExternalBenchmarkRunner {

    private final String imageName;
    private final String network;
    private final int warehouses;
    private final int transactionClients;
    private final int analyticalClients;
    private final int durationSeconds;


    public Ch2Runner(
            String imageName,
            String network,
            int warehouses,
            int transactionClients,
            int analyticalClients,
            int durationSeconds ) {
        super( "ch2", imageName, network );

        this.imageName = imageName;
        this.network = network;
        this.warehouses = warehouses;
        this.transactionClients = transactionClients;
        this.analyticalClients = analyticalClients;
        this.durationSeconds = durationSeconds;
    }


    @Override
    protected List<String> buildCommand() {
        return buildCh2Command( "--no-load" );
    }


    public void loadData( File outputDirectory ) {
        File logFile = new File( outputDirectory, "load.log" );

        DockerLauncher.runJob(
                "ch2-load-" + UUID.randomUUID(),
                imageName,
                Map.of(),
                network,
                buildCh2Command( "--no-execute" ),
                logFile
        );
    }


    private List<String> buildCh2Command( String phaseOption ) {
        List<String> command = new ArrayList<>();

        command.add( "python" );
        command.add( "./tpcc.py" );
        command.add( "--warehouses=" + warehouses );
        command.add( "--tclients=" + transactionClients );
        command.add( "--aclients=" + analyticalClients );
        command.add( "--duration=" + durationSeconds );
        command.add( phaseOption );
        command.add( "polypheny" );
        command.add( "--stop-on-error" );

        return command;
    }

    // no preperation needed probably
    @Override
    protected void prepareRun( File outputDirectory ) {
    }


    @Override
    public void analyze( Properties results, File outputDirectory ) {
        File logFile = new File( outputDirectory, "run.log" );
        results.setProperty( "ch2.runLog", logFile.getAbsolutePath() );
    }
}
