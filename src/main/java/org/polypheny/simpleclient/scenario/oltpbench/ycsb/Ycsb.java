package org.polypheny.simpleclient.scenario.oltpbench.ycsb;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.scenario.oltpbench.AbstractOltpBenchScenario;

@Slf4j
public class Ycsb extends AbstractOltpBenchScenario {

    public Ycsb( ExecutorFactory executorFactory, YcsbConfig config, boolean dumpQueryList, QueryMode queryMode ) {
        super( executorFactory, config, dumpQueryList, queryMode );
    }

}
