package cs.bilkent.joker.pipelinedFissionModel;

import java.util.ArrayList;
import java.util.List;

import cs.bilkent.joker.pipelinedFissionModel.scalabilityFunction.LinearScalabilityFunction;

public class ProgramGenerator
{
    public static ArrayList<Program> generateAllPrograms ( List<Operator> list, int maxThreads )
    {
        // phase 1: generate programs with regions that have a single replica and single pipeline
        ArrayList<Program> phase1Progs = generateAllSingleRepSinglePipe( list, maxThreads );
        // phase 2: generate programs with regions that have a single pipeline
        ArrayList<Program> phase2Progs = generateAllSinglePipe( phase1Progs, maxThreads );
        // phase 3: generate all programs
        ArrayList<Program> phase3Progs = generateAll( phase2Progs, maxThreads );
        return phase3Progs;
    }

    public static ArrayList<Program> generateAllPipelineOnly ( List<Operator> list, int maxThreads )
    {
        ArrayList<Program> progs = generateAllPrograms( list, maxThreads );
        ArrayList<Program> pipeOnly = new ArrayList<Program>();

        for ( int i = 0; i < progs.size(); i++ )
        {
            boolean pipelineOnly = true;
            int regionCount = progs.get( i ).getNumRegions();
            ArrayList<Region> regions = progs.get( i ).getRegions();

            for ( int regionIndex = 0; regionIndex < regionCount; regionIndex++ )
            {
                if ( regions.get( regionIndex ).getNumReplicas() != 1 )
                {
                    pipelineOnly = false;
                }
            }

            if ( pipelineOnly )
            {
                pipeOnly.add( progs.get( i ) );
            }

        }
        return pipeOnly;
    }

    public static ArrayList<Program> generateAllReplicationOnly ( List<Operator> list, int maxThreads )
    {
        ArrayList<Program> progs = generateAllPrograms( list, maxThreads );
        ArrayList<Program> replicationOnly = new ArrayList<Program>();

        final List<Integer> nonParallelizableRegionIndices = new ArrayList<>(  );
        ArrayList<Region> R = PipelinedFissionAlgorithm.configureRegions( list, 0, nonParallelizableRegionIndices);

        for ( int i = 0; i < progs.size(); i++ )
        {
            boolean repOnly = true;
            ArrayList<Region> regions = progs.get( i ).getRegions();
            int regionCount = progs.get( i ).getNumRegions();

            if ( regionCount == R.size() )
            {
                for ( int regionIndex = 0; regionIndex < regionCount; regionIndex++ )
                {
                    Region currentRegion = regions.get( regionIndex );

                    if ( currentRegion.getNumPipelines() != 1 )
                    {
                        repOnly = false;
                    }
                }

                if ( repOnly )
                {
                    for ( int regionIndex = 0; regionIndex < regionCount; regionIndex++ )
                    {
                        Region currentRegion = regions.get( regionIndex );
                        int operatorCountCurrent = currentRegion.getFirstPipeline().getOperators().size();
                        int operatorCount = R.get( regionIndex ).getFirstPipeline().getOperators().size();

                        if ( operatorCountCurrent != operatorCount )
                        {
                            repOnly = false;
                        }
                    }
                }

                if ( repOnly )
                {
                    for ( int regionIndex = 0; regionIndex < regionCount; regionIndex++ )
                    {
                        Region currentRegion = regions.get( regionIndex );
                        ArrayList<Operator> operatorCurrent = currentRegion.getFirstPipeline().getOperators();
                        ArrayList<Operator> operator = R.get( regionIndex ).getFirstPipeline().getOperators();

                        for ( int operatorIndex = 0; operatorIndex < operator.size(); operatorIndex++ )
                        {
                            if ( !( operator.get( operatorIndex ).getIndex() == operatorCurrent.get( operatorIndex ).getIndex() ) )
                            {
                                repOnly = false;
                            }
                        }
                    }
                }

            }
            else
            {
                repOnly = false;
            }

            if ( repOnly )
            {
                replicationOnly.add( progs.get( i ) );
            }
        }

        return replicationOnly;
    }

    static class ProgramCountPair
    {
        Program program;
        int count;
    }

    private static ArrayList<Program> generateAll ( ArrayList<Program> phase2Progs, int maxThreads )
    {
        ArrayList<ProgramCountPair> candidates = new ArrayList<>();
        for ( Program prog : phase2Progs )
        {
            ProgramCountPair candidate = new ProgramCountPair();
            candidate.program = prog;
            candidate.count = 0;
            for ( Region region : prog.getRegions() )
            {
                candidate.count += region.getNumReplicas();
            }
            candidates.add( candidate );
        }
        ArrayList<Program> results = new ArrayList<>();
        generateAll( 0, maxThreads, candidates, results );
        return results;
    }

    private static void generateAll ( int nRegionsProcessed,
                                      int maxThreads,
                                      ArrayList<ProgramCountPair> candidates,
                                      ArrayList<Program> results )
    {
        ArrayList<ProgramCountPair> newCandidates = new ArrayList<>();
        for ( ProgramCountPair candidate : candidates )
        {
            Region region = candidate.program.getRegions().get( nRegionsProcessed );
            Pipeline pipeline = region.getFirstPipeline();
            int numDivisions = Math.min( pipeline.getNumOperators(), 1 + ( maxThreads - candidate.count ) / region.getNumReplicas() );
            ArrayList<ArrayList<Range>> allDivisions = getAllDivisions( pipeline.getNumOperators(), numDivisions );
            for ( ArrayList<Range> ranges : allDivisions )
            {
                ProgramCountPair newCandidate = new ProgramCountPair();
                newCandidate.program = new Program();
                for ( int k = 0; k < nRegionsProcessed; ++k )
                {
                    newCandidate.program.addRegion( (Region) candidate.program.getRegions().get( k ).clone() );
                }
                Region newRegion = new Region();
                newRegion.setNumReplicas( region.getNumReplicas() );
                for ( Range range : ranges )
                {
                    Pipeline newPipeline = new Pipeline();
                    for ( int k = range.start; k <= range.end; ++k )
                    {
                        newPipeline.addOperator( pipeline.getOperators().get( k ) );
                    }
                    newRegion.addPipeline( newPipeline );
                }
                newCandidate.program.addRegion( newRegion );
                for ( int k = nRegionsProcessed + 1; k < candidate.program.getNumRegions(); ++k )
                {
                    newCandidate.program.addRegion( (Region) candidate.program.getRegions().get( k ).clone() );
                }
                newCandidate.count = candidate.count + newRegion.getNumReplicas() * ( newRegion.getNumPipelines() - 1 );
                if ( newCandidate.count == maxThreads || newCandidate.program.getNumRegions() == ( nRegionsProcessed + 1 ) )
                {
                    results.add( newCandidate.program );
                }
                else
                {
                    newCandidates.add( newCandidate );
                }
            }
        }
        candidates = newCandidates;
        if ( !candidates.isEmpty() )
        {
            generateAllSinglePipe( nRegionsProcessed + 1, maxThreads, candidates, results );
        }
    }

    private static ArrayList<Program> generateAllSinglePipe ( ArrayList<Program> phase1Progs, int maxThreads )
    {
        ArrayList<ProgramCountPair> candidates = new ArrayList<>();
        for ( Program prog : phase1Progs )
        {
            ProgramCountPair candidate = new ProgramCountPair();
            candidate.program = prog;
            candidate.count = prog.getNumRegions();
            candidates.add( candidate );
        }
        ArrayList<Program> results = new ArrayList<>();
        generateAllSinglePipe( 0, maxThreads, candidates, results );
        return results;
    }

    private static void generateAllSinglePipe ( int nRegionsProcessed,
                                                int maxThreads,
                                                ArrayList<ProgramCountPair> candidates,
                                                ArrayList<Program> results )
    {
        ArrayList<ProgramCountPair> newCandidates = new ArrayList<>();
        for ( ProgramCountPair candidate : candidates )
        {
            int maxExtraReplicas = maxThreads - candidate.count;
            if ( candidate.program.getRegions().get( nRegionsProcessed ).getKind() == StateKind.Stateful )
            {
                maxExtraReplicas = 0;
            }
            for ( int i = 0; i <= maxExtraReplicas; ++i )
            {
                ProgramCountPair newCandidate = new ProgramCountPair();
                newCandidate.program = (Program) candidate.program.clone();
                newCandidate.program.getRegions().get( nRegionsProcessed ).setNumReplicas( 1 + i );
                newCandidate.count = candidate.count + i;
                if ( newCandidate.count == maxThreads || newCandidate.program.getNumRegions() == ( nRegionsProcessed + 1 ) )
                {
                    results.add( newCandidate.program );
                }
                else
                {
                    newCandidates.add( newCandidate );
                }
            }
        }
        candidates = newCandidates;
        if ( !candidates.isEmpty() )
        {
            generateAllSinglePipe( nRegionsProcessed + 1, maxThreads, candidates, results );
        }
    }

    private static ArrayList<Program> generateAllSingleRepSinglePipe ( List<Operator> operators, int maxThreads )
    {
        ArrayList<ProgramCountPair> candidates = new ArrayList<>();
        ProgramCountPair candidate = new ProgramCountPair();
        candidate.program = new Program();
        candidate.count = 0;
        candidates.add( candidate );
        ArrayList<Program> results = new ArrayList<Program>();
        generateAllSingleRepSinglePipe( operators, maxThreads, candidates, results );
        return results;
    }

    private static void generateAllSingleRepSinglePipe ( List<Operator> operators,
                                                         int maxThreads,
                                                         ArrayList<ProgramCountPair> candidates,
                                                         ArrayList<Program> results )
    {
        ArrayList<ProgramCountPair> newCandidates = new ArrayList<>();
        for ( ProgramCountPair candidate : candidates )
        {
            int endStartIndex = candidate.count;
            if ( candidate.program.getNumRegions() == maxThreads - 1 ) // this is the last one
            {
                endStartIndex = operators.size() - 1;
            }
            for ( int i = endStartIndex; i < operators.size(); ++i )
            {
                Pipeline pipeline = new Pipeline();
                for ( int j = candidate.count; j <= i; ++j )
                {
                    pipeline.addOperator( operators.get( j ) );
                }
                Region region = new Region();
                region.addPipeline( pipeline );
                ProgramCountPair newCandidate = new ProgramCountPair();
                newCandidate.program = (Program) candidate.program.clone();
                newCandidate.program.addRegion( region );
                newCandidate.count = i + 1;
                if ( newCandidate.program.getNumRegions() == maxThreads || newCandidate.count == operators.size() )
                {
                    results.add( newCandidate.program );
                }
                else
                {
                    newCandidates.add( newCandidate );
                }
            }
        }
        candidates = newCandidates;
        if ( !candidates.isEmpty() )
        {
            generateAllSingleRepSinglePipe( operators, maxThreads, candidates, results );
        }
    }

    // inclusive
    static class Range
    {
        int start;
        int end;
    }


    static class RangesCountPair
    {
        ArrayList<Range> ranges;
        int count;
    }

    // get all divisions of [0..n-1] that contain [1..m] ranges
    static ArrayList<ArrayList<Range>> getAllDivisions ( int n, int m )
    {
        ArrayList<RangesCountPair> candidates = new ArrayList<>();
        RangesCountPair candidate = new RangesCountPair();
        candidate.ranges = new ArrayList<>();
        candidate.count = 0;
        candidates.add( candidate );
        ArrayList<ArrayList<Range>> results = new ArrayList<>();
        getAllDivisions( n, m, candidates, results );
        return results;
    }

    @SuppressWarnings( "unchecked" )
    private static void getAllDivisions ( int n, int m, ArrayList<RangesCountPair> candidates, ArrayList<ArrayList<Range>> results )
    {
        ArrayList<RangesCountPair> newCandidates = new ArrayList<>();
        for ( RangesCountPair candidate : candidates )
        {
            int endStartIndex = candidate.count;
            if ( candidate.ranges.size() == m - 1 ) // this is the last one
            {
                endStartIndex = n - 1;
            }
            for ( int i = endStartIndex; i < n; ++i )
            {
                Range range = new Range();
                range.start = candidate.count;
                range.end = i;
                RangesCountPair newCandidate = new RangesCountPair();
                newCandidate.ranges = (ArrayList<Range>) candidate.ranges.clone();
                newCandidate.ranges.add( range );
                newCandidate.count = i + 1;
                if ( newCandidate.ranges.size() == m || newCandidate.count == n )
                {
                    results.add( newCandidate.ranges );
                }
                else
                {
                    newCandidates.add( newCandidate );
                }
            }
        }
        candidates = newCandidates;
        if ( !candidates.isEmpty() )
        {
            getAllDivisions( n, m, candidates, results );
        }
    }

    public static void main ( String[] args )
    {
        int maxOpers = 12;
        int maxThreads = 9;
        ArrayList<Operator> operators = new ArrayList<>();
        LinearScalabilityFunction linearSF = new LinearScalabilityFunction();
        for ( int i = 0; i < maxOpers; ++i )
        {
            operators.add( new Operator( 1, 10, 1.0, StateKind.Stateless, linearSF ) );
        }
        for ( int o = 1; o <= maxOpers; ++o )
        {
            for ( int t = 1; t <= maxThreads; ++t )
            {
                ArrayList<Program> programs = ProgramGenerator.generateAllPrograms( operators.subList( 0, o ), t );
                System.out.println( o + " " + " " + t + " " + programs.size() );
            }
        }
    }
}

