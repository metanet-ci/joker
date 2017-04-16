package cs.bilkent.joker.engine.config;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

import com.typesafe.config.Config;

import cs.bilkent.joker.operator.impl.TuplesImpl;

public class RegionManagerConfig
{

    static final String CONFIG_NAME = "regionManager";

    static final String PIPELINE_TAIL_OPERATOR_OUTPUT_SUPPLIER_CLASS = "pipelineTailOperatorOutputSupplierClass";


    private Class<Supplier<TuplesImpl>> pipelineTailOperatorOutputSupplierClass;

    RegionManagerConfig ( final Config parentConfig )
    {
        final Config config = parentConfig.getConfig( CONFIG_NAME );
        final String className = config.getString( PIPELINE_TAIL_OPERATOR_OUTPUT_SUPPLIER_CLASS );
        try
        {

            this.pipelineTailOperatorOutputSupplierClass = (Class<Supplier<TuplesImpl>>) Class.forName( className );
        }
        catch ( ClassNotFoundException e )
        {
            throw new RuntimeException( className + " not found!", e );
        }
    }

    public Class<Supplier<TuplesImpl>> getPipelineTailOperatorOutputSupplierClass ()
    {
        return pipelineTailOperatorOutputSupplierClass;
    }

    public Supplier<TuplesImpl> newPipelineTailOperatorOutputSupplierInstance ( final int portCount )
    {
        try
        {
            final Constructor<Supplier<TuplesImpl>> constructor = pipelineTailOperatorOutputSupplierClass.getConstructor( int.class );
            return constructor.newInstance( portCount );
        }
        catch ( NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e )
        {
            throw new RuntimeException( "cannot create instance of " + pipelineTailOperatorOutputSupplierClass.getName(), e );
        }
    }

    @Override
    public String toString ()
    {
        return "RegionManagerConfig{" + "pipelineTailOperatorOutputSupplierClass=" + pipelineTailOperatorOutputSupplierClass + '}';
    }

}
