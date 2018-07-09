package cs.bilkent.joker.engine.pipeline.impl.downstreamcollector;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.pipeline.DownstreamCollector;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class CompositeDownstreamCollector implements DownstreamCollector
{

    private final DownstreamCollector[] collectors;

    private final int until;

    public CompositeDownstreamCollector ( final DownstreamCollector[] collectors )
    {
        checkArgument( collectors != null && collectors.length > 0 );
        this.collectors = Arrays.copyOf( collectors, collectors.length );
        this.until = ( collectors.length - 1 );
    }

    @Override
    public void accept ( final TuplesImpl tuples )
    {
        for ( int i = 0; i < until; i++ )
        {
            collectors[ i ].accept( tuples.shallowCopy() );
        }
        collectors[ until ].accept( tuples );
    }

    public DownstreamCollector[] getDownstreamCollectors ()
    {
        return Arrays.copyOf( collectors, collectors.length );
    }

}
