package cs.bilkent.joker.engine.pipeline.impl.downstreamcollector;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.engine.pipeline.DownstreamCollector;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class CompositeDownstreamCollector implements DownstreamCollector
{

    private final DownstreamCollector[] collectors;

    private final int size;

    public CompositeDownstreamCollector ( final DownstreamCollector[] collectors )
    {
        checkArgument( collectors != null && collectors.length > 0 );
        this.collectors = Arrays.copyOf( collectors, collectors.length );
        this.size = collectors.length;
    }

    @Override
    public void accept ( final TuplesImpl tuples )
    {
        collectors[ 0 ].accept( tuples );
        for ( int i = 1; i < size; i++ )
        {
            collectors[ i ].accept( tuples.copyForAttachment() );
        }
    }

    public DownstreamCollector[] getDownstreamCollectors ()
    {
        return Arrays.copyOf( collectors, collectors.length );
    }

}
