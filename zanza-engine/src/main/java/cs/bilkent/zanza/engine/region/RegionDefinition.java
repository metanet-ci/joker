package cs.bilkent.zanza.engine.region;

import java.util.ArrayList;
import java.util.List;

import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static java.util.Collections.unmodifiableList;

public class RegionDefinition
{

    private final OperatorType regionType;

    private final List<String> partitionFieldNames;

    private final List<OperatorDefinition> operators;

    public RegionDefinition ( final OperatorType regionType,
                              final List<String> partitionFieldNames,
                              final List<OperatorDefinition> operators )
    {
        this.regionType = regionType;
        this.partitionFieldNames = unmodifiableList( new ArrayList<>( partitionFieldNames ) );
        this.operators = unmodifiableList( new ArrayList<>( operators ) );
    }

    public OperatorType getRegionType ()
    {
        return regionType;
    }

    public List<String> getPartitionFieldNames ()
    {
        return partitionFieldNames;
    }

    public List<OperatorDefinition> getOperators ()
    {
        return operators;
    }

}
