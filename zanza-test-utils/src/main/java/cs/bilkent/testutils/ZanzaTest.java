package cs.bilkent.testutils;

import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

public abstract class ZanzaTest
{

    public static long DEFAULT_ASSERT_EVENTUALLY_TIMEOUT_SECONDS = 30;


    @FunctionalInterface
    public interface AssertTask
    {
        void run () throws Exception;
    }


    @Rule
    public final TestWatcher testWatcher = new TestWatcher()
    {
        private long start;

        @Override
        protected void starting ( final Description description )
        {
            System.out.println( "##### STARTING TEST: " + description.getMethodName() + " ####" );
            start = System.nanoTime();
            super.starting( description );
        }

        @Override
        protected void finished ( Description description )
        {
            final long durationNs = System.nanoTime() - start;
            final long durationMs = durationNs / 1_000_000;
            if ( durationMs > 0 )
            {
                System.out.println( "##### TEST: " + description.getMethodName() + " COMPLETED IN " + durationMs + " ms ####" );
            }
            else
            {
                System.out.println( "##### TEST: " + description.getMethodName() + " COMPLETED IN " + durationNs + " ns ####" );
            }

        }

    };


    public static Thread spawnThread ( final Runnable runnable )
    {
        final Thread thread = new Thread( runnable );
        thread.start();
        return thread;
    }

    public static void assertTrueEventually ( AssertTask task )
    {
        assertTrueEventually( task, DEFAULT_ASSERT_EVENTUALLY_TIMEOUT_SECONDS );
    }

    public static void assertTrueEventually ( AssertTask task, long timeoutSeconds )
    {
        AssertionError error = null;

        long iterations = timeoutSeconds * 10;
        int sleepMillis = 100;
        for ( int k = 0; k < iterations; k++ )
        {
            try
            {
                try
                {
                    task.run();
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
                return;
            }
            catch ( AssertionError e )
            {
                error = e;
            }

            sleepUninterruptibly( sleepMillis, TimeUnit.MILLISECONDS );
        }
        throw error;
    }

}
