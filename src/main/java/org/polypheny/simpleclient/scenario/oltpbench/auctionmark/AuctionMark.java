package org.polypheny.simpleclient.scenario.oltpbench.auctionmark;

import java.io.File;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.OltpBenchExecutor;
import org.polypheny.simpleclient.executor.OltpBenchExecutor.OltpBenchExecutorFactory;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.scenario.Scenario;
import org.polypheny.simpleclient.scenario.oltpbench.AbstractOltpBenchScenario;

@Slf4j
public class AuctionMark extends AbstractOltpBenchScenario {

    public AuctionMark( ExecutorFactory executorFactory, AuctionMarkConfig config, boolean dumpQueryList, QueryMode queryMode ) {
        super( executorFactory, config, dumpQueryList, queryMode );
    }

}
