package cs.bilkent.joker.engine.util.concurrent;

import org.junit.Test;

import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategyData.State;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BackoffIdleStrategyTest extends AbstractJokerTest
{

    private final BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy( 2, 3, 4, 1, 2 );

    @Test
    public void testIdle ()
    {
        assertEquals( State.NOT_IDLE, idleStrategy.getState() );
        assertFalse( idleStrategy.idle() );

        assertEquals( State.SPINNING, idleStrategy.getState() );
        assertFalse( idleStrategy.idle() );

        assertEquals( State.SPINNING, idleStrategy.getState() );
        assertFalse( idleStrategy.idle() );

        assertEquals( State.YIELDING, idleStrategy.getState() );
        assertFalse( idleStrategy.idle() );

        assertEquals( State.YIELDING, idleStrategy.getState() );
        assertFalse( idleStrategy.idle() );

        assertEquals( State.YIELDING, idleStrategy.getState() );
        assertFalse( idleStrategy.idle() );

        assertEquals( State.PARKING, idleStrategy.getState() );
        assertFalse( idleStrategy.idle() );

        assertEquals( State.PARKING, idleStrategy.getState() );
        assertFalse( idleStrategy.idle() );

        assertEquals( State.PARKING, idleStrategy.getState() );
        assertFalse( idleStrategy.idle() );

        assertEquals( State.PARKING, idleStrategy.getState() );
        assertTrue( idleStrategy.idle() );

        assertEquals( State.PARKING, idleStrategy.getState() );
        assertTrue( idleStrategy.idle() );
    }

    @Test
    public void testReset ()
    {
        testIdle();
        idleStrategy.reset();
        testIdle();
    }

}
