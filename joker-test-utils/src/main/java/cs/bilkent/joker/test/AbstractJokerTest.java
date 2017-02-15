package cs.bilkent.joker.test;

import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

public abstract class AbstractJokerTest
{

    public static final long DEFAULT_ASSERT_EVENTUALLY_TIMEOUT_IN_SECONDS = 30;


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
            super.starting( description );
            System.out.println( "+ STARTED: " + description.getMethodName() );
            start = System.nanoTime();
            super.starting( description );
        }

        @Override
        protected void succeeded ( Description description )
        {
            super.succeeded( description );

            final long durationNanos = System.nanoTime() - start;
            final long durationMicros = durationNanos / 1000;
            final long durationMillis = durationMicros / 1000;
            final long durationSeconds = durationMillis / 1000;
            final long duration = durationSeconds > 0
                                  ? durationSeconds
                                  : ( durationMillis > 0 ? durationMillis : ( durationMicros > 0 ? durationMicros : durationNanos ) );
            final String unit =
                    durationSeconds > 0 ? "secs" : ( durationMillis > 0 ? "millis" : ( durationMicros > 0 ? "micros" : "nanos" ) );
            System.out.println( "+ SUCCEEDED: " + description.getMethodName() + " IN " + duration + " " + unit );
        }

        @Override
        protected void failed ( Throwable e, Description description )
        {
            super.failed( e, description );

            final long durationNanos = System.nanoTime() - start;
            final long durationMicros = durationNanos / 1000;
            final long durationMillis = durationMicros / 1000;
            final long durationSeconds = durationMillis / 1000;
            final long duration = durationSeconds > 0
                                  ? durationSeconds
                                  : ( durationMillis > 0 ? durationMillis : ( durationMicros > 0 ? durationMicros : durationNanos ) );
            final String unit =
                    durationSeconds > 0 ? "secs" : ( durationMillis > 0 ? "millis" : ( durationMicros > 0 ? "micros" : "nanos" ) );
            System.out.println( "+ FAILED: " + description.getMethodName() + " IN " + duration + " " + unit );
        }

    };

    public static Thread spawnThread ( final Runnable runnable )
    {
        final Thread thread = new Thread( runnable );
        thread.start();
        return thread;
    }

    public static void assertTrueEventually ( final AssertTask task )
    {
        assertTrueEventually( task, DEFAULT_ASSERT_EVENTUALLY_TIMEOUT_IN_SECONDS );
    }

    public static void assertTrueEventually ( final AssertTask task, final long timeoutSeconds )
    {
        AssertionError error = null;

        final long iterations = timeoutSeconds * 10;
        final int sleepMillis = 100;
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
