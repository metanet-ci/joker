package cs.bilkent.joker.pcj;

import org.pcj.PCJ;
import org.pcj.StartPoint;
import org.pcj.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PCJMain
{
    public static final String PCJ_TASK_CLASS_SYS_PARAM = "pcjTask";

    public static final String PCJ_NODES_SYS_PARAM = "pcjNodes";

    public static final String PCJ_JOKER_FACTORY_SYS_PARAM = "pcjJokerFactory";


    private static final Logger LOGGER = LoggerFactory.getLogger( PCJMain.class );

    /*
        java -Djava.util.logging.config.file=logging.properties -DsshUser=???
        -DoverrideClasspath=joker-pcj-1.0-SNAPSHOT-jar-with-dependencies.jar -DpcjTask=cs.bilkent.joker.pcj.PCJJokerWrapper
        "-DpcjNodes=ip addresses seperated by ;" -DpcjJokerFactory=cs.bilkent.joker.pcj.StaticPCJJokerInstancefactory
        -classpath target/joker-pcj-1.0-SNAPSHOT-jar-with-dependencies.jar cs.bilkent.joker.pcj.PCJMain
     */
    public static void main ( String[] args ) throws InterruptedException, ClassNotFoundException
    {
        final Class<?> taskClazz = Class.forName( System.getProperty( PCJ_TASK_CLASS_SYS_PARAM ) );
        final String[] nodes = System.getProperty( PCJ_NODES_SYS_PARAM ).trim().split( ";" );

        PCJ.deploy( (Class<StartPoint>) taskClazz, (Class<Storage>) taskClazz, nodes );

        LOGGER.info( "Completed." );
    }

}
