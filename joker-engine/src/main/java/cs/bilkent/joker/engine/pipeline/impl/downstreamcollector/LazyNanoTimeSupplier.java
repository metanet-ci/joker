package cs.bilkent.joker.engine.pipeline.impl.downstreamcollector;

import java.util.function.LongSupplier;

public class LazyNanoTimeSupplier implements LongSupplier
{

    private boolean loaded;
    private long time;

    @Override
    public long getAsLong ()
    {
        if ( !loaded )
        {
            time = System.nanoTime();
        }

        return time;
    }

    public void reset ()
    {
        loaded = false;
    }

}
