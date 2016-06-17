package cs.bilkent.zanza.engine.pipeline.impl.tuplesupplier;

import java.util.function.Supplier;

import cs.bilkent.zanza.operator.impl.TuplesImpl;

public class NonCachedTuplesImplSupplier implements Supplier<TuplesImpl>
{

    private final int portCount;

    public NonCachedTuplesImplSupplier ( final int portCount )
    {
        this.portCount = portCount;
    }

    @Override
    public TuplesImpl get ()
    {
        return new TuplesImpl( portCount );
    }

}
