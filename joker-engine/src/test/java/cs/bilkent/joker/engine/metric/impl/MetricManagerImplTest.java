package cs.bilkent.joker.engine.metric.impl;

import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.codahale.metrics.MetricRegistry;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.exception.JokerException;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Collections.emptyList;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class MetricManagerImplTest extends AbstractJokerTest
{

    @Mock
    private MetricRegistry metricRegistry;

    @Mock
    private ThreadMXBean threadMXBean;

    @Mock
    private RuntimeMXBean runtimeMXBean;

    @Mock
    private OperatingSystemMXBean osMXBean;

    private MetricManagerImpl metricManager;

    @Before
    public void init ()
    {
        when( threadMXBean.isThreadCpuTimeSupported() ).thenReturn( true );
        when( threadMXBean.isThreadCpuTimeEnabled() ).thenReturn( true );
        when( osMXBean.getAvailableProcessors() ).thenReturn( 2 );

        metricManager = new MetricManagerImpl( new JokerConfig(),
                                               metricRegistry,
                                               threadMXBean,
                                               runtimeMXBean,
                                               osMXBean,
                                               new ThreadGroup( "Test" ) );
    }

    @After
    public void tearDown ()
    {
        try
        {
            metricManager.shutdown();
        }
        catch ( Exception ignored )
        {

        }
    }

    @Test
    public void shouldStartSuccessfully () throws InterruptedException
    {
        metricManager.start( 0, emptyList() );
    }

    @Test
    public void shouldNotStartMultipleTimes ()
    {
        metricManager.start( 0, emptyList() );
        try
        {
            metricManager.start( 0, emptyList() );
            fail();
        }
        catch ( JokerException ignored )
        {

        }
    }

    @Test( expected = JokerException.class )
    public void shouldNotPauseBeforeStart ()
    {
        metricManager.pause();
    }

    @Test
    public void shouldPause ()
    {
        metricManager.start( 0, emptyList() );
        metricManager.pause();
    }

    @Test
    public void shouldResumeAfterPause ()
    {
        metricManager.start( 0, emptyList() );
        metricManager.pause();
        metricManager.resume( 0, emptyList(), emptyList() );
    }

    @Test( expected = JokerException.class )
    public void shouldNotResumeBeforePause ()
    {
        metricManager.start( 0, emptyList() );
        metricManager.resume( 0, emptyList(), emptyList() );
    }

    @Test( expected = JokerException.class )
    public void shouldNotResumeBeforeStart ()
    {
        metricManager.resume( 0, emptyList(), emptyList() );
    }

    @Test
    public void shouldShutdownBeforeStart ()
    {
        metricManager.shutdown();
    }

    @Test
    public void shouldShutdownIdempotently () throws InterruptedException
    {
        metricManager.start( 0, emptyList() );

        metricManager.shutdown();
        metricManager.shutdown();
    }

    @Test
    public void shouldNotPauseAfterShutdown () throws InterruptedException
    {
        metricManager.start( 0, emptyList() );

        metricManager.shutdown();

        try
        {
            metricManager.pause();
            fail();
        }
        catch ( IllegalStateException ignored )
        {

        }
    }

    @Test
    public void shouldNotResumeAfterShutdown () throws InterruptedException
    {
        metricManager.start( 0, emptyList() );

        metricManager.shutdown();

        try
        {
            metricManager.resume( 0, emptyList(), emptyList() );
            fail();
        }
        catch ( IllegalStateException ignored )
        {

        }
    }

}
