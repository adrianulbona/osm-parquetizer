package io.github.adrianulbona.osm.parquet;

import org.apache.parquet.hadoop.ParquetWriter;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static java.lang.String.format;


public class ParquetSink<T extends Entity> implements Sink {

    private final Path source;
    private final EntityType entityType;
    private final List<Predicate<T>> filters;
    private final List<Observer<T>> observers;

    private ParquetWriter<T> writer;

    public ParquetSink(Path source, EntityType entityType) {
        this.source = source;
        this.entityType = entityType;
        this.filters = new ArrayList<>();
        this.observers = new ArrayList<>();
    }

    @Override
    public void initialize(Map<String, Object> metaData) {
        observers.forEach(Observer::started);
        final String pbfName = source.getFileName().toString();
        final String entityName = entityType.name().toLowerCase();
        final Path destination = source.getParent().resolve(format("%s.%s.parquet", pbfName, entityName));
        try {
            this.writer = ParquetWriterFactory.buildFor(destination.toAbsolutePath().toString(), entityType);
        } catch (IOException e) {
            throw new RuntimeException("Unable to build writers", e);
        }
    }

    @Override
    public void process(EntityContainer entityContainer) {
        try {
            if (this.entityType == entityContainer.getEntity().getType()) {
                final T entity = (T) entityContainer.getEntity();
                observers.forEach(o -> o.arrived(entity));
                if (filters.stream().noneMatch(filter -> filter.test(entity))) {
                    observers.forEach(o -> o.writing(entity));
                    writer.write(entity);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to write entity", e);
        }
    }

    @Override
    public void complete() {
        try {
            observers.forEach(Observer::ended);
            this.writer.close();
        } catch (IOException e) {
            throw new RuntimeException("Unable to close writers", e);
        }
    }

    @Override
    public void release() {

    }

    public void addFilter(Predicate<T> predicate) {
        this.filters.add(predicate);
    }

    public void removeFilter(Predicate<T> predicate) {
        this.filters.remove(predicate);
    }

    public void addObserver(Observer<T> observer) {
        this.observers.add(observer);
    }

    public void removeObserver(Observer<T> observer) {
        this.observers.remove(observer);
    }

    public interface Observer<T extends Entity> {

        void started();

        void arrived(T entity);

        void writing(T entity);

        void ended();
    }

    public static ParquetSink<Way> waysSink(Path source) {
        return new ParquetSink<>(source, EntityType.Way);
    }

    public static ParquetSink<Node> nodeSink(Path source) {
        return new ParquetSink<>(source, EntityType.Node);
    }

    public static ParquetSink<Node> relationsSink(Path source) {
        return new ParquetSink<>(source, EntityType.Relation);
    }
}
