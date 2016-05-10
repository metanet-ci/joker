package cs.bilkent.zanza.engine;

import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

public class TestUtils
{

    public static long DEFAULT_ASSERT_EVENTUALLY_TIMEOUT_SECONDS = 30;


    @FunctionalInterface
    public interface AssertTask
    {
        void run () throws Exception;
    }


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
