package cs.bilkent.zanza.operator.util;


public class Triple<T1, T2, T3>
{
    public final T1 _1;

    public final T2 _2;

    public final T3 _3;

    public static <T1, T2, T3> Triple<T1, T2, T3> of ( final T1 _1, final T2 _2, final T3 _3 )
    {
        return new Triple<>( _1, _2, _3 );
    }

    public Triple ( final T1 _1, final T2 _2, final T3 _3 )
    {
        this._1 = _1;
        this._2 = _2;
        this._3 = _3;
    }

    public int productArity ()
    {
        return 3;
    }

    public Object productElement ( int n ) throws IndexOutOfBoundsException
    {
        if ( n == 0 )
        {
            return this._1;
        }
        else if ( n == 1 )
        {
            return this._2;
        }
        else if ( n == 2 )
        {
            return this._3;
        }
        else
        {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public String toString ()
    {
        return "Tuple3{" +
               "_1=" + _1 +
               ", _2=" + _2 +
               ", _3=" + _3 +
               '}';
    }
}
