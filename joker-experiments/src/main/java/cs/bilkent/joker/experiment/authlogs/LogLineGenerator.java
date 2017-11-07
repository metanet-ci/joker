package cs.bilkent.joker.experiment.authlogs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import cs.bilkent.joker.utils.Triple;
import static java.lang.Math.ceil;
import static java.lang.System.arraycopy;
import static java.lang.Thread.currentThread;
import static java.util.Collections.shuffle;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static java.util.stream.Collectors.toList;

public class LogLineGenerator implements Runnable
{

    private final Random random = new Random();

    private final List<Triple<String, String, String[]>> lines;

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

    private final AtomicReference<List<Triple<String, String, String[]>>> logsRef = new AtomicReference<>();

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

    private List<Triple<String, String, String[]>> parseLines ( final List<String> lines )
    {
        return lines.stream().map( line -> {
            final String[] tokens = line.split( " " );
            final String host = tokens[ 0 ];
            final String service = tokens[ 1 ];
            String[] t = new String[ tokens.length - 2 ];
            arraycopy( tokens, 2, t, 0, t.length );
            return Triple.of( host, service, t );
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

    private List<Triple<String, String, String[]>> generate ()
    {
        final List<Triple<String, String, String[]>> vals = new ArrayList<>( batchSize );
        final int authFailureCount = (int) ceil( authFailureRatio * batchSize );

        for ( int i = 0; i < authFailureCount; i++ )
        {
            String[] msg = new String[ 15 ];
            msg[ 0 ] = "pam_unix(sshd:auth):";
            msg[ 1 ] = "authentication";
            msg[ 2 ] = "failure;";
            msg[ 3 ] = "logname=";
            msg[ 4 ] = "uid=";
            msg[ 5 ] = uids.get( uidIdx++ );
            msg[ 6 ] = "euid=";
            msg[ 7 ] = euids.get( euidIdx++ );
            msg[ 8 ] = "tty=";
            msg[ 9 ] = "ssh";
            msg[ 10 ] = "ruser=";
            msg[ 11 ] = "rhost=";
            msg[ 12 ] = rhosts.get( rhostIdx++ );
            msg[ 13 ] = "user=";
            msg[ 14 ] = users.get( userIdx++ );

            final Triple<String, String, String[]> tokens = Triple.of( "ubuntu", "sshd[5126]:", msg );

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
            final Triple<String, String, String[]> line = lines.get( j++ );
            vals.add( line );
            if ( j == lines.size() )
            {
                j = 0;
            }
        }

        shuffle( vals );

        return vals;
    }

    private void setLogs ( final List<Triple<String, String, String[]>> logs )
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

    List<Triple<String, String, String[]>> getLogs ()
    {
        final List<Triple<String, String, String[]>> logs = logsRef.get();
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
