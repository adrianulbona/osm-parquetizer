package io.github.adrianulbona.osm.parquet.convertor;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;

import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;


/**
 * Created by adrian.bona on 26/03/16.
 */
public class WayWriteSupport extends OsmEntityWriteSupport<Way> {

    private final PrimitiveType nodesType;

    public WayWriteSupport() {
        nodesType = new PrimitiveType(REPEATED, INT64, "nodes");
    }

    @Override
    protected MessageType getSchema() {
        final List<Type> attributes = new ArrayList<>(getCommonAttributes());
        attributes.add(nodesType);
        return new MessageType("way", attributes);
    }

    @Override
    protected void writeSpecificFields(Way record, int nextAvailableIndex) {
        recordConsumer.startField(nodesType.getName(), nextAvailableIndex);
        record.getWayNodes().forEach(wayNode -> recordConsumer.addLong(wayNode.getNodeId()));
        recordConsumer.endField(nodesType.getName(), nextAvailableIndex);
    }
}
