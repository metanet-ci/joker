package cs.bilkent.joker.engine.metric;

import org.junit.Test;

import cs.bilkent.joker.engine.metric.PipelineReplicaMeter.Ticker;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TickerTest extends AbstractJokerTest
{

    private final Ticker ticker = new Ticker( 1 );

    @Test
    public void shouldNotBeTickedInitially ()
    {
        assertFalse( ticker.isTicked() );
    }

    @Test
    public void shouldBeTickedWithEnoughCalls ()
    {
        ticker.tryTick();

        assertFalse( ticker.isTicked() );

        ticker.tryTick();

        assertTrue( ticker.isTicked() );

        ticker.tryTick();

        assertFalse( ticker.isTicked() );

        assertEquals( 3, ticker.getCount() );
    }

    @Test
    public void shouldResetTickState ()
    {
        ticker.tryTick();
        ticker.tryTick();

        assertTrue( ticker.isTicked() );
        assertEquals( 2, ticker.getCount() );

        ticker.reset();

        assertFalse( ticker.isTicked() );
        assertEquals( 0, ticker.getCount() );
    }

}
