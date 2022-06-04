package org.polypheny.simpleclient.scenario.oltpbench.smallbank;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.scenario.oltpbench.AbstractOltpBenchScenario;

@Slf4j
public class SmallBank extends AbstractOltpBenchScenario {

    public SmallBank( ExecutorFactory executorFactory, SmallBankConfig config, boolean dumpQueryList, QueryMode queryMode ) {
        super( executorFactory, config, dumpQueryList, queryMode );
    }

}
