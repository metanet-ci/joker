package cs.bilkent.joker.experiment;

import java.util.Random;
import java.util.function.Consumer;

import cs.bilkent.joker.operator.Tuple;

public class BeaconFn implements Consumer<Tuple>
{
    private final Random random;

    private final int keyRange;

    private final int tuplesPerKey;

    private int key;

    private int val;

    private int count;

    public BeaconFn ( final Random random, final int keyRange, final int tuplesPerKey )
    {
        this.random = random;
        this.keyRange = keyRange;
        this.tuplesPerKey = tuplesPerKey;
        this.val = random.nextInt( 10 );
    }

    @Override
    public void accept ( final Tuple tuple )
    {
        tuple.set( "key", key );
        double val = this.val;
        tuple.set( "val1", val );
        tuple.set( "val2", val );
        if ( count++ > tuplesPerKey )
        {
            key = random.nextInt( keyRange );
            this.val = random.nextInt( 10 );
            count = 0;
        }
    }

}
