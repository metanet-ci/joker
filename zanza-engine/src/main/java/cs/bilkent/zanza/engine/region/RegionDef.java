package cs.bilkent.zanza.engine.region;

import java.util.ArrayList;
import java.util.List;

import cs.bilkent.zanza.flow.OperatorDef;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static java.util.Collections.unmodifiableList;

public class RegionDef
{

    private final OperatorType regionType;

    private final List<String> partitionFieldNames;

    private final List<OperatorDef> operators;

    public RegionDef ( final OperatorType regionType, final List<String> partitionFieldNames, final List<OperatorDef> operators )
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

    public List<OperatorDef> getOperators ()
    {
        return operators;
    }

    @Override
    public String toString ()
    {
        return "RegionDef{" +
               "regionType=" + regionType +
               ", partitionFieldNames=" + partitionFieldNames +
               ", operators=" + operators +
               '}';
    }

}
