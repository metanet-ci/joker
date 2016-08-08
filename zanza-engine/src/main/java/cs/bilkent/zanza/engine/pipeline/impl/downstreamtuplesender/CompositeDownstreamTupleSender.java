package cs.bilkent.zanza.engine.pipeline.impl.downstreamtuplesender;

import java.util.Arrays;
import java.util.concurrent.Future;

import cs.bilkent.zanza.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.zanza.operator.impl.TuplesImpl;

public class CompositeDownstreamTupleSender implements DownstreamTupleSender
{

    private final DownstreamTupleSender[] senders;

    private final int size;

    public CompositeDownstreamTupleSender ( final DownstreamTupleSender[] senders )
    {
        this.senders = Arrays.copyOf( senders, senders.length );
        this.size = senders.length;
    }

    @Override
    public Future<Void> send ( final TuplesImpl tuples )
    {
        for ( int i = 0; i < size; i++ )
        {
            senders[ i ].send( tuples );
        }

        return null;
    }

    public DownstreamTupleSender[] getDownstreamTupleSenders ()
    {
        return Arrays.copyOf( senders, senders.length );
    }

}
