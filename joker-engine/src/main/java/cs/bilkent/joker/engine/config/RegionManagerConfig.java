package cs.bilkent.joker.engine.config;

import java.util.function.Supplier;

import com.typesafe.config.Config;

import cs.bilkent.joker.operator.impl.TuplesImpl;

public class RegionManagerConfig
{

    public static final String CONFIG_NAME = "regionManager";

    public static final String PIPELINE_TAIL_OPERATOR_OUTPUT_SUPPLIER_CLASS = "pipelineTailOperatorOutputSupplierClass";


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

}
