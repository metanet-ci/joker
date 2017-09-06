package cs.bilkent.joker.experiment.authlogs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Math.ceil;
import static java.lang.System.arraycopy;
import static java.lang.Thread.currentThread;
import static java.util.Collections.shuffle;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class LogLineGenerator implements Runnable
{

    private final Random random = new Random();

    private final List<String[]> lines;

    private final int batchSize;

    private final int uidRange;

    private final List<String> uids;

    private final int euidRange;

    private final List<String> euids;

    private final int rhostCount;

    private final List<String> rhosts;

    private final int userCount;

    private final List<String> users;

    private final double authFailureRatio;

    private int uidIdx;

    private int euidIdx;

    private int rhostIdx;

    private int userIdx;

    private final AtomicReference<List<String[]>> logsRef = new AtomicReference<>();

    private volatile boolean running = true;

    LogLineGenerator ( final List<String> lines,
                       final int batchSize,
                       final int uidRange,
                       final int euidRange,
                       final int rhostCount,
                       final int userCount,
                       final double authFailureRatio )
    {
        this.lines = parseLines( lines );
        this.batchSize = batchSize;
        this.uidRange = uidRange;
        this.uids = new ArrayList<>( uidRange );
        this.euidRange = euidRange;
        this.euids = new ArrayList<>( euidRange );
        this.rhostCount = rhostCount;
        this.rhosts = new ArrayList<>( rhostCount );
        this.userCount = userCount;
        this.users = new ArrayList<>( userCount );
        this.authFailureRatio = authFailureRatio;
        init();
    }

    private List<String[]> parseLines ( final List<String> lines )
    {
        return lines.stream().map( line -> {
            final String[] tokens = line.split( " " );
            final String host = tokens[ 0 ];
            final String service = tokens[ 1 ];
            final String message = Arrays.stream( tokens ).skip( 2 ).collect( joining( " " ) );

            String[] output = new String[ 3 ];
            output[ 0 ] = host;
            output[ 1 ] = service;
            output[ 2 ] = message;

            return output;
        } ).collect( toList() );
    }

    private void init ()
    {
        for ( int i = 0; i < uidRange; i++ )
        {
            uids.add( "uid" + String.valueOf( i ) );
        }

        shuffle( uids );

        for ( int i = 0; i < euidRange; i++ )
        {
            euids.add( "euid" + String.valueOf( i ) );
        }

        shuffle( euids );

        for ( int i = 0; i < userCount; i++ )
        {
            users.add( "user" + String.valueOf( i ) );
        }

        shuffle( users );

        final Set<String> rhosts = new HashSet<>();
        while ( rhosts.size() < rhostCount )
        {
            rhosts.add( random.nextInt( 256 ) + "." + random.nextInt( 256 ) + "." + random.nextInt( 256 ) + "." + random.nextInt( 256 ) );
        }

        this.rhosts.addAll( rhosts );

        shuffle( this.rhosts );
    }

    @Override
    public void run ()
    {
        while ( running )
        {
            setLogs( generate() );
        }
    }

    private List<String[]> generate ()
    {
        final List<String[]> vals = new ArrayList<>( batchSize );
        final int authFailureCount = (int) ceil( authFailureRatio * batchSize );

        for ( int i = 0; i < authFailureCount; i++ )
        {
            String[] tokens = new String[ 3 ];
            tokens[ 0 ] = "ubuntu";
            tokens[ 1 ] = "sshd[5126]:";

            final StringBuilder sb = new StringBuilder();
            sb.append( "pam_unix(sshd:auth): authentication failure; logname= uid=" ).append( uids.get( uidIdx++ ) );
            sb.append( " euid=" ).append( euids.get( euidIdx++ ) ).append( " tty=ssh ruser= rhost=" ).append( rhosts.get( rhostIdx++ ) );
            sb.append( " user=" ).append( users.get( userIdx++ ) );
            tokens[ 2 ] = sb.toString();

            vals.add( tokens );

            if ( uidIdx == uidRange )
            {
                uidIdx = 0;
            }

            if ( euidIdx == euidRange )
            {
                euidIdx = 0;
            }

            if ( rhostIdx == rhostCount )
            {
                rhostIdx = 0;
            }

            if ( userIdx == userCount )
            {
                userIdx = 0;
            }
        }

        for ( int i = authFailureCount, j = random.nextInt( lines.size() ); i < batchSize; i++ )
        {
            final String[] line = lines.get( j++ );
            final String[] output = new String[] { line[ 0 ], line[ 1 ], line[ 2 ] };

            arraycopy( line, 0, output, 0, output.length );

            vals.add( output );
            if ( j == lines.size() )
            {
                j = 0;
            }
        }

        shuffle( vals );

        return vals;
    }

    private void setLogs ( final List<String[]> logs )
    {
        while ( !logsRef.compareAndSet( null, logs ) )
        {
            if ( !running )
            {
                break;
            }

            parkNanos( 100 );
        }
    }

    List<String[]> getLogs ()
    {
        final List<String[]> logs = logsRef.get();
        if ( logs != null )
        {
            logsRef.compareAndSet( logs, null );
            return logs;
        }

        return null;
    }

    void shutdown ( final Thread threadToJoin )
    {
        running = false;

        try
        {
            threadToJoin.join( TimeUnit.SECONDS.toMillis( 30 ) );
        }
        catch ( InterruptedException e )
        {
            currentThread().interrupt();
            throw new RuntimeException( e );
        }
    }

}
