package cs.bilkent.zanza.engine.util.concurrent;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * A NON RE-ENTRANT spin lock implementation for read-write scenarios.
 * Both read lock and write lock attempts apply back off if they fail to acquire the lock repeatedly.
 * Still, high number of readers / writers may cause temporary CPU bursts.
 * <p>
 * Please be aware that this is a very simple implementation and should be used with care. For instance, if you attempt to unlock
 * a lock without acquiring it first, you will not get an exception.
 */
public class ReadWriteSpinLock
{

    private static final int WRITE_LOCK = -1;

    private static final int NO_LOCK = 0;


    private final AtomicInteger lock = new AtomicInteger( NO_LOCK );


    public void lockRead ()
    {
        int idleCount = 0, val;
        for ( ; ; )
        {
            val = lock.get();
            if ( val >= NO_LOCK && lock.compareAndSet( val, val + 1 ) )
            {
                break;
            }
            else
            {
                backOff( idleCount++ );
            }
        }
    }

    public void unlockRead ()
    {
        lock.decrementAndGet();
    }

    public void lockWrite ()
    {
        int idleCount = 0, val;
        while ( true )
        {
            val = lock.get();
            if ( val == NO_LOCK )
            {
                if ( lock.compareAndSet( NO_LOCK, WRITE_LOCK ) )
                {
                    break;
                }
            }
            else if ( val > NO_LOCK )
            {
                // Acquire the write lock even though there are readers so no new readers / writers will be able to acquire the lock.
                if ( lock.compareAndSet( val, WRITE_LOCK ) )
                {
                    final int waitUntil = WRITE_LOCK - val;

                    // Wait until ongoing readers to finish their work
                    for ( ; ; )
                    {
                        if ( lock.get() == waitUntil )
                        {
                            break;
                        }
                        else
                        {
                            backOff( idleCount++ );
                        }
                    }

                    // Reset the lock to the write lock
                    lock.set( WRITE_LOCK );
                }
            }
            else
            {
                backOff( idleCount++ );
            }
        }
    }

    public void unlockWrite ()
    {
        lock.compareAndSet( WRITE_LOCK, NO_LOCK );
    }

    private void backOff ( final int count )
    {
        if ( count < 1000 )
        {
            // busy spin
        }
        else if ( count < 1100 )
        {
            Thread.yield();
        }
        else if ( count < 1200 )
        {
            LockSupport.parkNanos( 1 );
        }
        else
        {
            try
            {
                Thread.sleep( 1 );
            }
            catch ( InterruptedException ignore )
            {
            }
        }
    }

}
