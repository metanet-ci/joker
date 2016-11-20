package cs.bilkent.joker.engine.region.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

import cs.bilkent.joker.operator.impl.TuplesImpl;

public final class OutputSupplierUtils
{

    public static Supplier<TuplesImpl> newInstance ( final Class<Supplier<TuplesImpl>> clazz, final int inputPortCount )
    {
        try
        {
            final Constructor<Supplier<TuplesImpl>> constructor = clazz.getConstructor( int.class );
            return constructor.newInstance( inputPortCount );
        }
        catch ( NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e )
        {
            throw new RuntimeException( "cannot create instance of " + clazz.getName(), e );
        }
    }

    private OutputSupplierUtils ()
    {

    }

}
