package cs.bilkent.joker.operator.impl;

import cs.bilkent.joker.operator.Tuple;

public class DefaultOutputTupleCollector implements OutputTupleCollector
{

    private final TuplesImpl output;

    public DefaultOutputTupleCollector ( final int portCount )
    {
        this( new TuplesImpl( portCount ) );
    }

    public DefaultOutputTupleCollector ( final TuplesImpl output )
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
