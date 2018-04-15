package cs.bilkent.joker.engine.pipeline.impl.invocation;

import java.util.List;

import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.TupleAccessor;
import cs.bilkent.joker.operator.impl.OutputCollector;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class DefaultOutputCollector implements OutputCollector
{

    private final TuplesImpl output;

    public DefaultOutputCollector ( final int portCount )
    {
        this( new TuplesImpl( portCount ) );
    }

    public DefaultOutputCollector ( final TuplesImpl output )
    {
        this.output = output;
    }

    @Override
    public void add ( final Tuple tuple )
    {
        output.add( tuple );
    }

    @Override
    public void add ( final int portIndex, final Tuple tuple )
    {
        output.add( portIndex, tuple );
    }

    @Override
    public void recordInvocationLatency ( final String operatorId, final long latency )
    {
        for ( int i = 0; i < output.getPortCount(); i++ )
        {
            final List<Tuple> tuples = output.getTuplesModifiable( i );
            for ( int j = 0; j < tuples.size(); j++ )
            {
                TupleAccessor.recordInvocationLatency( tuples.get( j ), operatorId, latency );
            }
        }
    }

    @Override
    public TuplesImpl getOutputTuples ()
    {
        return output;
    }

    @Override
    public void clear ()
    {
        output.clear();
    }

}
