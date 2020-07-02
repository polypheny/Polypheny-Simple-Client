package org.polypheny.simpleclient.query;

import com.google.gson.JsonObject;

public abstract class BatchableInsert extends Query {

    public BatchableInsert( boolean expectResult ) {
        super( expectResult );
    }


    public abstract String getSqlRowExpression();

    public abstract JsonObject getRestRowExpression();

    public abstract String getTable();
}
