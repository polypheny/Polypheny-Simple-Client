package org.polypheny.simpleclient.query;

import com.google.gson.JsonObject;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;


public abstract class BatchableInsert extends Query {

    public BatchableInsert( boolean expectResult ) {
        super( expectResult );
    }


    public abstract String getSqlRowExpression();

    public abstract String getParameterizedSqlQuery();

    public abstract Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues();

    public abstract JsonObject getRestRowExpression();

    public abstract String getTable();


    public enum DataTypes {INTEGER, VARCHAR, TIMESTAMP, DATE, ARRAY_INT, ARRAY_REAL}

}
