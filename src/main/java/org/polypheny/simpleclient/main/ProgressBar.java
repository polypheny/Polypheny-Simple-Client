package org.polypheny.simpleclient.main;


/**
 * Taken from: http://masterex.github.io/archive/2011/10/23/java-cli-progress-bar.html
 */
public class ProgressBar extends ProgressReporter {

    private StringBuilder progress;
    private volatile boolean finished;


    /**
     * initialize progress bar properties.
     */
    public ProgressBar( int numberOfThreads, int base ) {
        super( numberOfThreads, base );
        init();
    }


    /**
     * Called whenever the progress bar needs to be updated.
     * That is whenever progress was made.
     *
     * @param done an int representing the work done so far
     * @param total an int representing the total work
     */
    @Override
    public void update( int done, int total ) {
        char[] workchars = { '|', '/', '-', '\\' };
        String format = "\r%3d%% %s %c";

        int percent = (++done * 100) / total;
        int extrachars = (percent / 2) - this.progress.length();

        while ( extrachars-- > 0 ) {
            progress.append( '#' );
        }

        if ( !finished ) {
            System.out.printf( format, percent, progress, workchars[done % workchars.length] );

            if ( done == total ) {
                finished = true;
                System.out.flush();
                System.out.println();
                init();
            }
        }
    }


    private void init() {
        this.progress = new StringBuilder( 60 );
    }


    /**
     * @param progress Progress in thousandth
     */
    @Override
    protected void update( int progress ) {
        update( progress, base );
    }

}
