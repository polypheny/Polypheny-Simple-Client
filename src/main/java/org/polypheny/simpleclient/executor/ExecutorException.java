package org.polypheny.simpleclient.executor;


public class ExecutorException extends Exception {

    public ExecutorException( Exception e ) {
        super( e );
    }


    public ExecutorException( String message, Exception e ) {
        super( message, e );
    }
}
