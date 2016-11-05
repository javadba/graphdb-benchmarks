package eu.socialsensor.insert;

import eu.socialsensor.graphdatabases.HBaseGraphDatabase;
import eu.socialsensor.main.GraphDatabaseType;
import io.hgraphdb.HBaseBulkLoader;
import io.hgraphdb.HBaseGraph;
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
    private final HBaseGraph graph;
    private final HBaseBulkLoader loader;
    Map<Integer, Vertex> cache = new HashMap<Integer, Vertex>();

    public HBaseMassiveInsertion(HBaseGraph graph)
    {
        super(GraphDatabaseType.HBASE, null /* resultsPath */);
        this.graph = graph;
        this.loader = new HBaseBulkLoader(graph);
    }

    @Override
    protected Vertex getOrCreate(String value)
    {
        final Integer intValue = Integer.valueOf(value);
        Vertex vertex = cache.get(intValue);
        if (vertex == null)
        {
            vertex = loader.addVertex(T.label, HBaseGraphDatabase.NODE_LABEL, NODEID, intValue);
            cache.put(intValue, vertex);
        }
        return vertex;
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest)
    {
        loader.addEdge(src, dest, SIMILAR);
    }

    @Override
    protected void post()
    {
    }
}
