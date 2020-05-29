package org.polypheny.simpleclient.query;

import kong.unirest.HttpRequest;
import lombok.Getter;

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

}
