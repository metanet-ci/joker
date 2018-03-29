package cs.bilkent.joker.engine.pipeline.impl.downstreamcollector;

import java.util.Arrays;

import cs.bilkent.joker.engine.pipeline.DownstreamCollector;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class CompositeDownstreamCollector implements DownstreamCollector
{

    private final DownstreamCollector[] collectors;

    private final int size;

    public CompositeDownstreamCollector ( final DownstreamCollector[] collectors )
    {
        this.collectors = Arrays.copyOf( collectors, collectors.length );
        this.size = collectors.length;
    }

    @Override
    public void accept ( final TuplesImpl tuples )
    {
        for ( int i = 0; i < size; i++ )
        {
            collectors[ i ].accept( tuples );
        }
    }

    public DownstreamCollector[] getDownstreamCollectors ()
    {
        return Arrays.copyOf( collectors, collectors.length );
    }

}
