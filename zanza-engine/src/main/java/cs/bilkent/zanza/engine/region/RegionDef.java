package cs.bilkent.zanza.engine.region;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.flow.OperatorDef;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static java.util.Collections.unmodifiableList;

public class RegionDef
{

    private final int regionId;

    private final OperatorType regionType;

    private final List<String> partitionFieldNames;

    private final List<OperatorDef> operators;

    public RegionDef ( final int regionId,
                       final OperatorType regionType,
                       final List<String> partitionFieldNames,
                       final List<OperatorDef> operators )
    {
        checkArgument( regionType != null, "region type cannot be null" );
        checkArgument( ( regionType == PARTITIONED_STATEFUL && partitionFieldNames.size() > 0 ) || partitionFieldNames.isEmpty(),
                       "invalid partition field names! regionType: %s partition field names: %s",
                       regionType,
                       partitionFieldNames );
        checkArgument( operators != null && operators.size() > 0, "no operators!" );
        this.regionId = regionId;
        this.regionType = regionType;
        this.partitionFieldNames = unmodifiableList( new ArrayList<>( partitionFieldNames ) );
        this.operators = unmodifiableList( new ArrayList<>( operators ) );
    }

    public int getRegionId ()
    {
        return regionId;
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

    public int getOperatorCount ()
    {
        return operators.size();
    }

    public OperatorDef getFirstOperator ()
    {
        return operators.get( 0 );
    }

    public OperatorDef getLastOperator ()
    {
        return operators.get( operators.size() - 1 );
    }

    @Override
    public boolean equals ( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        final RegionDef regionDef = (RegionDef) o;

        if ( regionId != regionDef.regionId )
        {
            return false;
        }
        if ( regionType != regionDef.regionType )
        {
            return false;
        }
        if ( !partitionFieldNames.equals( regionDef.partitionFieldNames ) )
        {
            return false;
        }
        return operators.equals( regionDef.operators );

    }

    @Override
    public int hashCode ()
    {
        int result = regionId;
        result = 31 * result + regionType.hashCode();
        result = 31 * result + partitionFieldNames.hashCode();
        result = 31 * result + operators.hashCode();
        return result;
    }

    @Override
    public String toString ()
    {
        return "RegionDef{" + "regionId=" + regionId + ", regionType=" + regionType + ", partitionFieldNames=" + partitionFieldNames
               + ", operators=" + operators + '}';
    }
}
