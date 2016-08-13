package pcj;

import org.pcj.PCJ;
import org.pcj.StartPoint;
import org.pcj.Storage;

public class PCJHelloWorld extends Storage implements StartPoint
{
    @Override
    public void main () throws Throwable
    {
        System.out.println( "Hello from " + PCJ.myId() + " of " + PCJ.threadCount() );
        PCJ.barrier();
        System.out.println( "After barrier " + PCJ.myId() );
    }

    public static void main ( String[] args )
    {
        String[] nodes = new String[] { "localhost", "localhost", "localhost", "localhost" };
        PCJ.deploy( PCJHelloWorld.class, PCJHelloWorld.class, nodes );
    }

}
