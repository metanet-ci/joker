package cs.bilkent.joker.test.pipelinedFissionModel;

import java.util.ArrayList;
import java.util.HashSet;

public class StateKindMerger
{
    public static class StateKindAndKeys
    {
        StateKind kind;
        ArrayList<String> keys;
    }

    static StateKindAndKeys mergeLeft ( StateKind kind, ArrayList<String> keys, StateKind oKind, ArrayList<String> oKeys )
    {
        StateKindAndKeys result = new StateKindAndKeys();
        if ( kind == StateKind.Stateful || oKind == StateKind.Stateful )
        {
            result.kind = StateKind.Stateful;
        }
        else if ( oKind == StateKind.Stateless )
        {
            result.kind = kind;
            result.keys = keys;
        }
        else if ( kind == StateKind.Stateless )
        {
            if ( oKeys != null )
            {
                result.keys = new ArrayList<>();
                result.keys.addAll( oKeys );
            }
            result.kind = oKind;
        }
        else
        { // (kind==StateKind.PartitionedStateful) && (oKind == StateKind.PartitionedStateful)
            int mSize = keys.size();
            int oSize = oKeys.size();
            if ( mSize <= oSize )
            {
                HashSet<String> oKeysH = new HashSet<>();
                oKeysH.addAll( oKeys );
                boolean oHasAll = true;
                for ( String key : keys )
                {
                    if ( !oKeysH.contains( key ) )
                    {
                        oHasAll = false;
                        break;
                    }
                }
                if ( !oHasAll )
                {
                    result.kind = StateKind.Stateful;
                }
                else
                {
                    result.kind = StateKind.PartitionedStateful;
                    if ( mSize == oSize )
                    {
                        result.keys = keys;
                    }
                    else
                    {
                        keys = new ArrayList<>();
                        keys.addAll( oKeys );
                    }
                }
            }
            else
            { // (oSize<mSize)
                HashSet<String> mKeysH = new HashSet<>();
                mKeysH.addAll( keys );
                boolean mHasAll = true;
                for ( String key : oKeys )
                {
                    if ( !mKeysH.contains( key ) )
                    {
                        mHasAll = false;
                        break;
                    }
                }
                if ( !mHasAll )
                {
                    result.kind = StateKind.Stateful;
                }
                else
                {
                    result.kind = StateKind.PartitionedStateful;
                    result.keys = keys;
                }
            }
        }
        return result;
    }
}
