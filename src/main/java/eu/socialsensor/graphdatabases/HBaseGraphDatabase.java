package eu.socialsensor.graphdatabases;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import eu.socialsensor.insert.HBaseMassiveInsertion;
import eu.socialsensor.insert.HBaseSingleInsertion;
import eu.socialsensor.insert.Insertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphConfiguration;
import io.hgraphdb.IndexType;
import io.hgraphdb.OperationType;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * HBase graph database implementation.
 *
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class HBaseGraphDatabase extends GraphDatabaseBase<Iterator<Vertex>, Iterator<Edge>, Vertex, Edge>
{

    private static final Logger LOG = LogManager.getLogger();

    public static final String UNIQUE_HASH_INDEX = "UNIQUE_HASH_INDEX";
    public static final String NOTUNIQUE_HASH_INDEX = "NOTUNIQUE_HASH_INDEX";
    private final HBaseGraph graph;

    @SuppressWarnings("deprecation")
    public HBaseGraphDatabase(BenchmarkConfiguration config, File dbStorageDirectoryIn)
    {
        super(GraphDatabaseType.HBASE, dbStorageDirectoryIn, config.getRandomNodeList(),
                config.getShortestPathMaxHops());
        graph = getGraph(dbStorageDirectory);
        createSchema();
    }

    @Override
    public void massiveModeLoading(File dataPath)
    {
        HBaseMassiveInsertion hbaseMassiveInsertion = new HBaseMassiveInsertion(graph);
        hbaseMassiveInsertion.createGraph(dataPath, 0 /* scenarioNumber */);
    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber)
    {
        Insertion hbaseSingleInsertion = new HBaseSingleInsertion(this.graph, resultsPath);
        hbaseSingleInsertion.createGraph(dataPath, scenarioNumber);
    }

    @Override
    public void shutdown()
    {
        try
        {
            graph.close();
        } catch(Exception e) {
            throw new IllegalStateException("unable to close graph", e);
        }
    }

    @Override
    public void delete()
    {
        graph.drop();

        Utils.deleteRecursively(dbStorageDirectory);
    }

    @Override
    public void shutdownMassiveGraph()
    {
        shutdown();
    }

    @Override
    public void shortestPath(final Vertex fromNode, Integer targetNode)
    {
        final GraphTraversalSource g = graph.traversal();
        final Stopwatch watch = Stopwatch.createStarted();
        final DepthPredicate maxDepth = new DepthPredicate(maxHops);
        final Integer fromNodeId = fromNode.<Integer>value(NODE_ID);
        LOG.trace("finding path from {} to {} max hops {}", fromNodeId, targetNode, maxHops);
        final GraphTraversal<?, Path> t =
                g.V().has(NODE_LABEL, NODE_ID, fromNodeId)
                        .repeat(
                                __.out(SIMILAR)
                                        .simplePath())
                        .until(
                                __.has(NODE_LABEL, NODE_ID, targetNode)
                                        .and(__.filter( maxDepth ))
                        )
                        .limit(1)
                        .path();

        t.tryNext()
                .ifPresent( it -> {
                    final int pathSize = it.size();
                    final long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
                    watch.stop();
                    if(elapsed > 2000) { //threshold for debugging
                        LOG.warn("from @ " + fromNode.value(NODE_ID) +
                                " to @ " + targetNode.toString() +
                                " took " + elapsed + " ms, " + pathSize + ": " + it.toString());
                    }
                });
    }

    @Override
    public int getNodeCount()
    {
        return graph.traversal().V().count().toList().get(0).intValue();
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId)
    {
        final Set<Integer> neighbours = new HashSet<Integer>();
        final Vertex vertex = getVertex(nodeId);
        vertex.vertices(Direction.IN, SIMILAR).forEachRemaining(new Consumer<Vertex>() {
            @Override
            public void accept(Vertex t) {
                Integer neighborId = (Integer) t.property(NODE_ID).value();
                neighbours.add(neighborId);
            }
        });
        return neighbours;
    }

    @Override
    public double getNodeWeight(int nodeId)
    {
        Vertex vertex = getVertex(nodeId);
        double weight = getNodeOutDegree(vertex);
        return weight;
    }

    public double getNodeInDegree(Vertex vertex)
    {
        return (double) Iterators.size(vertex.edges(Direction.IN, SIMILAR));
    }

    public double getNodeOutDegree(Vertex vertex)
    {
        return (double) Iterators.size(vertex.edges(Direction.OUT, SIMILAR));
    }

    @Override
    public void initCommunityProperty()
    {
        int communityCounter = 0;
        for (Vertex v : graph.traversal().V().toList())
        {
            v.property(NODE_COMMUNITY, communityCounter);
            v.property(COMMUNITY, communityCounter);
            communityCounter++;
        }
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunity)
    {
        Set<Integer> communities = new HashSet<Integer>();

        for (Vertex vertex : graph.traversal().V().has(NODE_LABEL, NODE_COMMUNITY, nodeCommunity).toList())
        {
            final Iterator<Vertex> it = vertex.vertices(Direction.OUT, SIMILAR);
            for (Vertex v; it.hasNext();)
            {
                v = it.next();
                int community = (Integer) v.property(COMMUNITY).value();
                if (!communities.contains(community))
                {
                    communities.add(community);
                }
            }
        }
        return communities;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community)
    {
        Set<Integer> nodes = new HashSet<Integer>();
        for (Vertex v : graph.traversal().V().has(NODE_LABEL, COMMUNITY, community).toList())
        {
            Integer nodeId = (Integer) v.property(NODE_ID).value();
            nodes.add(nodeId);
        }
        return nodes;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity)
    {
        Set<Integer> nodes = new HashSet<Integer>();
        for (Vertex v : graph.traversal().V().has(NODE_LABEL, NODE_COMMUNITY, nodeCommunity).toList())
        {
            Integer nodeId = (Integer) v.property(NODE_ID).value();
            nodes.add(nodeId);
        }
        return nodes;
    }

    @Override
    public double getEdgesInsideCommunity(int vertexCommunity, int communityVertices)
    {
        double edges = 0;
        Set<Vertex> comVertices = graph.traversal().V().has(NODE_LABEL, COMMUNITY, communityVertices).toSet();
        for (Vertex vertex : graph.traversal().V().has(NODE_LABEL, NODE_COMMUNITY, vertexCommunity).toList())
        {
            Iterator<Vertex> it = vertex.vertices(Direction.OUT, SIMILAR);
            for (Vertex v; it.hasNext();)
            {
                v = it.next();
                if(comVertices.contains(v)) {
                    edges++;
                }
            }
        }
        return edges;
    }

    @Override
    public double getCommunityWeight(int community)
    {
        double communityWeight = 0;
        final List<Vertex> list = graph.traversal().V().has(NODE_LABEL, COMMUNITY, community).toList();
        if (list.size() <= 1) {
            return communityWeight;
        }
        for (Vertex vertex : list)
        {
            communityWeight += getNodeOutDegree(vertex);
        }
        return communityWeight;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity)
    {
        double nodeCommunityWeight = 0;
        for (Vertex vertex : graph.traversal().V().has(NODE_LABEL, NODE_COMMUNITY, nodeCommunity).toList())
        {
            nodeCommunityWeight += getNodeOutDegree(vertex);
        }
        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int nodeCommunity, int toCommunity)
    {
        for (Vertex vertex : graph.traversal().V().has(NODE_LABEL, NODE_COMMUNITY, nodeCommunity).toList())
        {
            vertex.property(COMMUNITY, toCommunity);
        }
    }

    @Override
    public double getGraphWeightSum()
    {
        final Iterator<Edge> edges = graph.edges();
        return (double) Iterators.size(edges);
    }

    @Override
    public int reInitializeCommunities()
    {
        Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
        int communityCounter = 0;
        Iterator<Vertex> it = graph.vertices();
        for (Vertex v; it.hasNext();)
        {
            v = it.next();
            int communityId = (Integer) v.property(COMMUNITY).value();
            if (!initCommunities.containsKey(communityId))
            {
                initCommunities.put(communityId, communityCounter);
                communityCounter++;
            }
            int newCommunityId = initCommunities.get(communityId);
            v.property(COMMUNITY, newCommunityId);
            v.property(NODE_COMMUNITY, newCommunityId);
        }
        return communityCounter;
    }

    @Override
    public int getCommunity(int nodeCommunity)
    {
        Vertex vertex = graph.traversal().V().has(NODE_LABEL, NODE_COMMUNITY, nodeCommunity).next();
        int community = (Integer) vertex.property(COMMUNITY).value();
        return community;
    }

    @Override
    public int getCommunityFromNode(int nodeId)
    {
        Vertex vertex = getVertex(nodeId);
        return (Integer) vertex.property(COMMUNITY).value();
    }

    @Override
    public int getCommunitySize(int community)
    {
        Set<Integer> nodeCommunities = new HashSet<Integer>();
        for (Vertex v : graph.traversal().V().has(NODE_LABEL, COMMUNITY, community).toList())
        {
            int nodeCommunity = (Integer) v.property(NODE_COMMUNITY).value();
            if (!nodeCommunities.contains(nodeCommunity))
            {
                nodeCommunities.add(nodeCommunity);
            }
        }
        return nodeCommunities.size();
    }

    @Override
    public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities)
    {
        Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();
        for (int i = 0; i < numberOfCommunities; i++)
        {
            GraphTraversal<Vertex, Vertex> t = graph.traversal().V().has(NODE_LABEL, COMMUNITY, i);
            List<Integer> vertices = new ArrayList<Integer>();
            while (t.hasNext())
            {
                Integer nodeId = (Integer) t.next().property(NODE_ID).value();
                vertices.add(nodeId);
            }
            communities.put(i, vertices);
        }
        return communities;
    }

    protected void createSchema()
    {
        createIndex(NODE_ID, NODE_LABEL);
        createIndex(COMMUNITY, NODE_LABEL);
        createIndex(NODE_COMMUNITY, NODE_LABEL);
    }

    private void createIndex(String key, String label) {
        if (graph.hasIndex(OperationType.READ, IndexType.VERTEX, label, key)) {
            return;
        }
        graph.createIndex(IndexType.VERTEX, label, key);
    }

    private static final boolean useMock = false;
    private HBaseGraph getGraph(final File dbPath)
    {
        HBaseGraphConfiguration config = new HBaseGraphConfiguration()
                .setGraphNamespace(dbPath.getName())
                .setCreateTables(true)
                .setLazyLoading(true);
        if (useMock) {
            config.setInstanceType(HBaseGraphConfiguration.InstanceType.MOCK);
        } else {
            config.setInstanceType(HBaseGraphConfiguration.InstanceType.DISTRIBUTED);
            config.setProperty("hbase.zookeeper.quorum", "127.0.0.1");
            config.setProperty("zookeeper.znode.parent", "/hbase-unsecure");
            /*
            config.setProperty("hbase.zookeeper.quorum", "stagehbase-2.az1.dm2.yammer.com,stagehbase-2.az2.dm2.yammer.com,stagehbase-2.az3.dm2.yammer.com");
            config.setProperty("zookeeper.znode.parent", "/hbase-secure");
            config.setRegionCount(128);
            config.setCompressionAlgorithm("snappy");
            config.setProperty("hbase.security.authentication", "kerberos");
            config.setProperty("hadoop.security.authentication",  "kerberos");
            config.setProperty("hbase.master.kerberos.principal",  "hbase/_HOST@STAGEHBASE.DM2.YAMMER.COM");
            config.setProperty("hbase.regionserver.kerberos.principal",  "hbase/_HOST@STAGEHBASE.DM2.YAMMER.COM");
            config.setProperty("hbase.client.kerberos.principal",  "hbase-stagehbasedm2@STAGEHBASE.DM2.YAMMER.COM");
            config.setProperty("hbase.client.keytab.file",  "/etc/security/keytabs/hbase.headless.keytab");
            config.setProperty("hbase.rpc.protection",  "authentication");
            */
        }
        return new HBaseGraph(config);
    }

    @Override
    public Iterator<Vertex> getVertexIterator()
    {
        return graph.vertices();
    }

    @Override
    public Iterator<Edge> getNeighborsOfVertex(Vertex v)
    {
        return v.edges(Direction.BOTH, SIMILAR);
    }

    @Override
    public void cleanupVertexIterator(Iterator<Vertex> it)
    {
        // NOOP for timing
    }

    @Override
    public Vertex getOtherVertexFromEdge(Edge edge, Vertex oneVertex)
    {
        return edge.inVertex().equals(oneVertex) ? edge.outVertex() : edge.inVertex();
    }

    @Override
    public Iterator<Edge> getAllEdges()
    {
        return graph.edges();
    }

    @Override
    public Vertex getSrcVertexFromEdge(Edge edge)
    {
        return edge.outVertex();
    }

    @Override
    public Vertex getDestVertexFromEdge(Edge edge)
    {
        return edge.inVertex();
    }

    @Override
    public boolean edgeIteratorHasNext(Iterator<Edge> it)
    {
        return it.hasNext();
    }

    @Override
    public Edge nextEdge(Iterator<Edge> it)
    {
        return it.next();
    }

    @Override
    public void cleanupEdgeIterator(Iterator<Edge> it)
    {
        // NOOP
    }

    @Override
    public boolean vertexIteratorHasNext(Iterator<Vertex> it)
    {
        return it.hasNext();
    }

    @Override
    public Vertex nextVertex(Iterator<Vertex> it)
    {
        return it.next();
    }

    @Override
    public Vertex getVertex(Integer i)
    {
        final GraphTraversalSource g = graph.traversal();
        final Vertex vertex = g.V().has(NODE_LABEL, NODE_ID, i).next();
        return vertex;
    }
}
