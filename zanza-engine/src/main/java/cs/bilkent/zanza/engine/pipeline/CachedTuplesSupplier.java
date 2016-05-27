package cs.bilkent.zanza.engine.pipeline;

import java.util.function.Supplier;

import cs.bilkent.zanza.operator.impl.TuplesImpl;

public class CachedTuplesSupplier implements Supplier<TuplesImpl>
{

    private final TuplesImpl tuples;

    public CachedTuplesSupplier ( final TuplesImpl tuples )
    {
        this.tuples = tuples;
    }

    @Override
    public TuplesImpl get ()
    {
        tuples.clear();
        return tuples;
    }

}
