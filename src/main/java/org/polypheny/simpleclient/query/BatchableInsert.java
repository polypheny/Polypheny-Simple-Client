package org.polypheny.simpleclient.query;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;


public abstract class BatchableInsert extends Query {

    public BatchableInsert( boolean expectResult ) {
        super( expectResult );
    }


    public abstract String getSqlRowExpression();

    public abstract JsonObject getRestRowExpression();


    public String getMongoQlRowExpression() {
        List<String> fields = new ArrayList<>();
        for ( Entry<String, JsonElement> entry : getRestRowExpression().entrySet() ) {
            String[] splits = entry.getKey().split( "\\." );
            fields.add( "\"" + splits[splits.length - 1] + "\"" + ":" + maybeQuote( entry.getValue() ) );
        }
        return "{" + String.join( ",", fields ) + "}";
    }


    protected String maybeQuote( JsonElement value ) {
        if ( value.isJsonPrimitive() && value.getAsJsonPrimitive().isString() ) {
            return "\"" + value.getAsString() + "\"";
        } else {
            return value.getAsString();
        }
    }


    public abstract String getTable();

}
