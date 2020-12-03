package org.polypheny.simpleclient.query;

import java.util.Map;
import kong.unirest.HttpRequest;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class RawQuery extends Query {

    @Getter
    private final String sql;

    @Getter
    private final HttpRequest<?> rest;


    public RawQuery( String sql, HttpRequest<?> rest, boolean expectResultSet ) {
        super( expectResultSet );
        this.sql = sql;
        this.rest = rest;
    }


    @Override
    public String getParameterizedSqlQuery() {
        return null;
    }


    @Override
    public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
        return null;
    }

}
