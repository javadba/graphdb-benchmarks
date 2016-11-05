package eu.socialsensor.insert;

import eu.socialsensor.graphdatabases.HBaseGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of massive Insertion in HBase graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public class HBaseMassiveInsertion extends InsertionBase<Vertex> implements Insertion
{
    private final Graph graph;
    Map<Integer, Vertex> cache = new HashMap<Integer, Vertex>();

    public HBaseMassiveInsertion(Graph graph)
    {
        super(GraphDatabaseType.HBASE, null /* resultsPath */);
        this.graph = graph;
    }

    @Override
    protected Vertex getOrCreate(String value)
    {
        final Integer intValue = Integer.valueOf(value);
        Vertex vertex = cache.get(intValue);
        if (vertex == null)
        {
            vertex = graph.addVertex(T.label, HBaseGraphDatabase.NODE_LABEL, NODEID, intValue);
            cache.put(intValue, vertex);
        }
        return vertex;
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest)
    {
        src.addEdge(SIMILAR, dest);
    }

    @Override
    protected void post()
    {
    }
}
