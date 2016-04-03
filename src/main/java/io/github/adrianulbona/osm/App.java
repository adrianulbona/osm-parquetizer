package io.github.adrianulbona.osm;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import crosby.binary.osmosis.OsmosisReader;
import io.github.adrianulbona.osm.parquet.ParquetSink;
import org.apache.parquet.Log;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.Files.newInputStream;


/**
 * Created by adrian.bona on 27/03/16.
 */
public class App {

    public static void main(String[] args) throws IOException {
//        final Path source = Paths.get("data", "planet-latest.osm.pbf");
        final Path source = Paths.get("data", "romania-latest.osm.pbf");

        //final Path source = Paths.get("data", "hungary-150908.osm.pbf");
        //final Path source = Paths.get("data", "great-britain-latest.osm.pbf");
/*


        final ParquetSink<Way> waysSink = ParquetSink.waysSink(source);

        //waysSink.addFilter(way -> way.getTags().stream().noneMatch(tag -> "highway".equals(tag.getKey())));
        final WaysSinkObserver observer = new WaysSinkObserver();
        //waysSink.addObserver(observer);
        processPbf(source, waysSink);

        ParquetSink<Node> nodeSink = ParquetSink.nodeSink(source);
        //nodeSink.addFilter(observer.referredNodes::containsValue);
        processPbf(source, nodeSink);
*/

        final MultiEntitySink allSink = new MultiEntitySink(source);
        processPbf(source, allSink);
    }

    public static void processPbf(Path source, Sink sink) throws IOException {
        final OsmosisReader reader = new OsmosisReader(newInputStream(source));
        reader.setSink(sink);
        reader.run();
    }

    private static class WaysSinkObserver implements ParquetSink.Observer<Way> {

        private static final Log LOGGER = Log.getLog(ParquetSink.class);

        private AtomicLong totalEntitiesCount;
        private AtomicLong totalWrittenEntitiesCount;
        private AtomicLong totalPointsAllWays;
        private AtomicLong totalPointsAllWrittenWays;

        public Multimap<Integer, Long> referredNodes;


        @Override
        public void started() {
            referredNodes = HashMultimap.create();
            totalEntitiesCount = new AtomicLong();
            totalWrittenEntitiesCount = new AtomicLong();
            totalPointsAllWays = new AtomicLong();
            totalPointsAllWrittenWays = new AtomicLong();
        }

        @Override
        public void arrived(Way entity) {
            totalEntitiesCount.incrementAndGet();
            totalPointsAllWays.addAndGet(entity.getWayNodes().size());
        }

        @Override
        public void writing(Way entity) {
            totalWrittenEntitiesCount.incrementAndGet();
            totalPointsAllWrittenWays.addAndGet(entity.getWayNodes().size());
            entity.getWayNodes().forEach(node -> referredNodes.put((int) (node.getNodeId() % 1000), node.getNodeId()));
        }

        @Override
        public void ended() {
            LOGGER.info("Total entities processed: " + totalEntitiesCount.get());
            LOGGER.info("Total entities written: " + totalWrittenEntitiesCount.get());
            LOGGER.info("Total referenced points: " + totalPointsAllWays.get());
            LOGGER.info("Total referenced points from entities written: " + totalPointsAllWrittenWays.get());
            referredNodes.keySet().forEach(key -> System.out.println(key + " -> " + referredNodes.get(key).size()));
        }
    }
}
