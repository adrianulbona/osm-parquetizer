package io.github.adrianulbona.osm.parquet;

import crosby.binary.osmosis.OsmosisReader;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.Files.newInputStream;
import static java.util.Collections.unmodifiableList;
import static org.openstreetmap.osmosis.core.domain.v0_6.EntityType.Node;
import static org.openstreetmap.osmosis.core.domain.v0_6.EntityType.Relation;


/**
 * Created by adrian.bona on 27/03/16.
 */
public class App {

    public static void main(String[] args) throws IOException {
        final MultiEntitySinkConfig config = new MultiEntitySinkConfig();
        final CmdLineParser cmdLineParser = new CmdLineParser(config);
        try {
            cmdLineParser.parseArgument(args);
            final OsmosisReader reader = new OsmosisReader(newInputStream(config.getSource()));
            final MultiEntitySink sink = new MultiEntitySink(config);
            sink.addObserver(new MultiEntitySinkObserver());
            reader.setSink(sink);
            reader.run();
        } catch (CmdLineException e) {
            System.out.println(e.getMessage());
            System.out.print("Usage: java -jar osm-parquetizer.jar");
            System.out.println();
            cmdLineParser.printSingleLineUsage(System.out);
        }
    }

    private static class MultiEntitySinkConfig implements MultiEntitySink.Config {

        @Argument(index = 0, metaVar = "pbf-path", usage = "the OSM PBF file to be parquetized", required = true)
        private Path source;

        @Argument(index = 1, metaVar = "output-path", usage = "the directory where to store the Parquet files",
                required = false)
        private Path destinationFolder;

        @Option(name = "--no-nodes", usage = "if present the nodes will be not parquetized")
        private boolean noNodes = false;

        @Option(name = "--no-ways", usage = "if present the ways will be not parquetized")
        private boolean noWays = false;

        @Option(name = "--no-relations", usage = "if present the relations will not be parquetized")
        private boolean noRelations = false;

        @Override
        public Path getSource() {
            return this.source;
        }

        @Override
        public Path getDestinationFolder() {
            return this.destinationFolder != null ? this.destinationFolder : this.source.toAbsolutePath().getParent();
        }

        @Override
        public List<EntityType> entitiesToBeParquetized() {
            final List<EntityType> entityTypes = new ArrayList<>();
            if (!noNodes) {
                entityTypes.add(Node);
            }
            if (!noWays) {
                entityTypes.add(EntityType.Way);
            }
            if (!noRelations) {
                entityTypes.add(Relation);
            }
            return unmodifiableList(entityTypes);
        }
    }


    private static class MultiEntitySinkObserver implements MultiEntitySink.Observer {

        private static final Logger LOGGER = LoggerFactory.getLogger(MultiEntitySinkObserver.class);

        private AtomicLong totalEntitiesCount;

        @Override
        public void started() {
            totalEntitiesCount = new AtomicLong();
        }

        @Override
        public void processed(Entity entity) {
            final long count = totalEntitiesCount.incrementAndGet();
            if (count % 1000000 == 0) {
                LOGGER.info("Entities processed: " + count);

            }
        }

        @Override
        public void ended() {
            LOGGER.info("Total entities processed: " + totalEntitiesCount.get());
        }
    }
}
