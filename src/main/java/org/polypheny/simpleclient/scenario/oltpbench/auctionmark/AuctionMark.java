package org.polypheny.simpleclient.scenario.oltpbench.auctionmark;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.scenario.oltpbench.AbstractOltpBenchScenario;

@Slf4j
public class AuctionMark extends AbstractOltpBenchScenario {

    public AuctionMark( ExecutorFactory executorFactory, AuctionMarkConfig config, boolean dumpQueryList, QueryMode queryMode ) {
        super( executorFactory, config, dumpQueryList, queryMode );
    }

}
