package cs.bilkent.joker.engine.region;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.spec.OperatorType;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

public class RegionDef
{

    private static final Map<OperatorType, Set<OperatorType>> REGION_OPERATOR_TYPES;

    static
    {
        final Map<OperatorType, Set<OperatorType>> m = new HashMap<>();
        m.put( STATELESS, EnumSet.of( STATELESS ) );
        m.put( PARTITIONED_STATEFUL, EnumSet.of( PARTITIONED_STATEFUL, STATELESS ) );
        m.put( STATEFUL, EnumSet.of( STATEFUL, STATELESS ) );
        REGION_OPERATOR_TYPES = unmodifiableMap( m );
    }

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
        checkArgument( regionType == PARTITIONED_STATEFUL ? partitionFieldNames.size() > 0 : partitionFieldNames.isEmpty(),
                       "invalid partition field names! regionType: %s partition field names: %s",
                       regionType,
                       partitionFieldNames );
        checkArgument( operators != null && operators.size() > 0, "no operators!" );
        checkOperatorTypes( regionType, operators );
        this.regionId = regionId;
        this.regionType = regionType;
        this.partitionFieldNames = unmodifiableList( new ArrayList<>( partitionFieldNames ) );
        this.operators = unmodifiableList( new ArrayList<>( operators ) );
    }

    private void checkOperatorTypes ( final OperatorType regionType, final List<OperatorDef> operators )
    {
        final Set<OperatorType> allowedOperatorTypes = REGION_OPERATOR_TYPES.get( regionType );
        for ( OperatorDef operator : operators )
        {
            checkArgument( allowedOperatorTypes.contains( operator.operatorType() ),
                           "%s Operator: %s is not allowed in %s region. Allowed operator types: %s",
                           operator.operatorType(),
                           operator.id(),
                           regionType,
                           allowedOperatorTypes );
        }
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

        return regionId == regionDef.regionId && regionType == regionDef.regionType
               && partitionFieldNames.equals( regionDef.partitionFieldNames ) && operators.equals( regionDef.operators );
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
