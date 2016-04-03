package io.github.adrianulbona.osm.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.openstreetmap.osmosis.core.domain.v0_6.*;

import java.io.IOException;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;


/**
 * Created by adrian.bona on 26/03/16.
 */
public class ParquetWriterFactory {

    private static final CompressionCodecName COMPRESSION = SNAPPY;

    public static <T extends Entity> ParquetWriter<T> buildFor(String destination, EntityType entityType) throws
            IOException {
        switch (entityType) {
            case Node:
                return (ParquetWriter<T>) NodesWriterBuilder.standard(destination);
            case Way:
                return (ParquetWriter<T>) WaysWriterBuilder.standard(destination);
            case Relation:
                return (ParquetWriter<T>) RelationsWriterBuilder.standard(destination);
            default:
                throw new RuntimeException("Invalid entity type");
        }
    }

    public static class WaysWriterBuilder extends ParquetWriter.Builder<Way, WaysWriterBuilder> {

        protected WaysWriterBuilder(Path file) {
            super(file);
        }

        @Override
        protected WaysWriterBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<Way> getWriteSupport(Configuration conf) {
            return new WayWriteSupport();
        }

        public static ParquetWriter<Way> standard(String destination) throws IOException {
            return new WaysWriterBuilder(new Path(destination)).self().withCompressionCodec(COMPRESSION)
                    .withWriteMode(OVERWRITE).build();
        }
    }


    public static class NodesWriterBuilder extends ParquetWriter.Builder<Node, NodesWriterBuilder> {

        protected NodesWriterBuilder(Path file) {
            super(file);
        }

        @Override
        protected NodesWriterBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<Node> getWriteSupport(Configuration conf) {
            return new NodeWriteSupport();
        }

        public static ParquetWriter<Node> standard(String destination) throws IOException {
            return new NodesWriterBuilder(new Path(destination)).self().withCompressionCodec(COMPRESSION)
                    .withWriteMode(OVERWRITE).build();
        }
    }


    public static class RelationsWriterBuilder extends ParquetWriter.Builder<Relation, RelationsWriterBuilder> {

        protected RelationsWriterBuilder(Path file) {
            super(file);
        }

        @Override
        protected RelationsWriterBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<Relation> getWriteSupport(Configuration conf) {
            return new RelationWriteSupport();
        }

        public static ParquetWriter<Relation> standard(String destination) throws IOException {
            return new RelationsWriterBuilder(new Path(destination)).self().withCompressionCodec(COMPRESSION)
                    .withWriteMode(OVERWRITE).build();
        }
    }
}
