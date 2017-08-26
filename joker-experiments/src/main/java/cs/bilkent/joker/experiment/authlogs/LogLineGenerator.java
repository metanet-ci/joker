package cs.bilkent.joker.experiment.authlogs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Math.ceil;
import static java.lang.Thread.currentThread;
import static java.util.Collections.shuffle;
import static java.util.concurrent.locks.LockSupport.parkNanos;

public class LogLineGenerator implements Runnable
{

    private final Random random = new Random();

    private final List<String> lines;

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

    private final AtomicReference<List<String>> logsRef = new AtomicReference<>();

    private volatile boolean running = true;

    LogLineGenerator ( final List<String> lines,
                       final int batchSize,
                       final int uidRange,
                       final int euidRange,
                       final int rhostCount,
                       final int userCount,
                       final double authFailureRatio )
    {
        this.lines = new ArrayList<>( lines );
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

    private List<String> generate ()
    {
        final List<String> vals = new ArrayList<>( batchSize );
        final int authFailureCount = (int) ceil( authFailureRatio * batchSize );

        for ( int i = 0; i < authFailureCount; i++ )
        {
            final StringBuilder sb = new StringBuilder();
            sb.append( "ubuntu sshd[5126]: pam_unix(sshd:auth): authentication failure; logname= uid=" ).append( uids.get( uidIdx++ ) );
            sb.append( " euid=" ).append( euids.get( euidIdx++ ) ).append( " tty=ssh ruser= rhost=" ).append( rhosts.get( rhostIdx++ ) );
            sb.append( " user=" ).append( users.get( userIdx++ ) );

            vals.add( sb.toString() );

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
            vals.add( lines.get( j++ ) );
            if ( j == lines.size() )
            {
                j = 0;
            }
        }

        shuffle( vals );

        return vals;
    }

    private void setLogs ( final List<String> logs )
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

    List<String> getLogs ()
    {
        final List<String> logs = logsRef.get();
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
