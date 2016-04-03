package io.github.adrianulbona.osm.parquet;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.lifecycle.Completable;
import org.openstreetmap.osmosis.core.lifecycle.Releasable;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;


/**
 * Created by adrian.bona on 27/03/16.
 */
public class MultiEntitySink implements Sink {

    private final List<ParquetSink<Entity>> converters;

    private final List<Observer> observers;

    public MultiEntitySink(Config config) {
        final Path pbfPath = config.getSource();
        final List<EntityType> entityTypes = config.entitiesToBeParquetized();
        this.converters = entityTypes.stream().map(type -> new ParquetSink<>(pbfPath, type)).collect(toList());
        this.observers = new ArrayList<>();
    }

    @Override
    public void process(EntityContainer entityContainer) {
        this.converters.forEach(converter -> converter.process(entityContainer));
        this.observers.forEach(o -> o.processed(entityContainer.getEntity()));
    }

    @Override
    public void initialize(Map<String, Object> metaData) {
        this.converters.forEach(converter -> converter.initialize(metaData));
        this.observers.forEach(Observer::started);
    }

    @Override
    public void complete() {
        this.converters.forEach(Completable::complete);
        this.observers.forEach(Observer::ended);
    }

    @Override
    public void release() {
        this.converters.forEach(Releasable::release);
    }

    public void addObserver(Observer observer) {
        this.observers.add(observer);
    }

    public void removeObserver(Observer observer) {
        this.observers.remove(observer);
    }

    public interface Observer {

        void started();

        void processed(Entity entity);

        void ended();
    }


    public interface Config {

        Path getSource();

        List<EntityType> entitiesToBeParquetized();
    }
}
