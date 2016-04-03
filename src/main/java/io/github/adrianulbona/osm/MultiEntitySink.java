package io.github.adrianulbona.osm;

import io.github.adrianulbona.osm.parquet.ParquetSink;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.lifecycle.Completable;
import org.openstreetmap.osmosis.core.lifecycle.Releasable;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * Created by adrian.bona on 27/03/16.
 */
class MultiEntitySink implements Sink {

    private final List<ParquetSink<Entity>> converters;

    public MultiEntitySink(Path source) {
        this.converters = Arrays.asList(new ParquetSink<>(source, EntityType.Node),
                new ParquetSink<>(source, EntityType.Way),
                new ParquetSink<>(source, EntityType.Relation));
    }

    @Override
    public void process(EntityContainer entityContainer) {
        this.converters.forEach(converter -> converter.process(entityContainer));
    }

    @Override
    public void initialize(Map<String, Object> metaData) {
        this.converters.forEach(converter -> converter.initialize(metaData));
    }

    @Override
    public void complete() {
        this.converters.forEach(Completable::complete);
    }

    @Override
    public void release() {
        this.converters.forEach(Releasable::release);
    }
}
