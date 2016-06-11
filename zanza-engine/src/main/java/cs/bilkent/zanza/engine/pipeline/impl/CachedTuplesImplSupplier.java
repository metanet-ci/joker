package cs.bilkent.zanza.engine.pipeline.impl;

import java.util.function.Supplier;

import cs.bilkent.zanza.operator.impl.TuplesImpl;

public class CachedTuplesImplSupplier implements Supplier<TuplesImpl>
{

    private final TuplesImpl tuples;

    public CachedTuplesImplSupplier ( final int portCount )
    {
        this.tuples = new TuplesImpl( portCount );
    }

    @Override
    public TuplesImpl get ()
    {
        tuples.clear();
        return tuples;
    }

}
