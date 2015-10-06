package cs.bilkent.zanza.operator.util;


public class Pair<T1, T2>
{

    public final T1 _1;

    public final T2 _2;

    public static <T1, T2> Pair<T1, T2> of ( final T1 _1, final T2 _2 )
    {
        return new Pair<>( _1, _2 );
    }

    public Pair ( final T1 _1, final T2 _2 )
    {
        this._1 = _1;
        this._2 = _2;
    }

    public int productArity ()
    {
        return 2;
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
        else
        {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public String toString ()
    {
        return "Tuple2{" +
               "_1=" + _1 +
               ", _2=" + _2 +
               '}';
    }
}
