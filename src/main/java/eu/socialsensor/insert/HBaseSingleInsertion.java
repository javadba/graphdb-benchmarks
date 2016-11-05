package eu.socialsensor.insert;

import eu.socialsensor.main.GraphDatabaseType;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.File;

/**
 * Implementation of single Insertion in HBase graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public final class HBaseSingleInsertion extends InsertionBase<Vertex>
{
    protected final Graph graph;

    public HBaseSingleInsertion(Graph graph, File resultsPath)
    {
        super(GraphDatabaseType.HBASE, resultsPath);
        this.graph = graph;
    }

    @Override
    protected void relateNodes(Vertex src, Vertex dest)
    {
        try
        {
            src.addEdge(SIMILAR, dest);
        }
        catch (Exception e)
        {
        }
    }

    protected Vertex getOrCreate(final String value) {
        final Integer intValue = Integer.valueOf(value);
        final GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().has(NODE_LABEL, NODEID, intValue);
        final Vertex vertex = traversal.hasNext() ? traversal.next() : graph.addVertex(T.label, NODE_LABEL, NODEID, intValue);
        return vertex;
    }

}
