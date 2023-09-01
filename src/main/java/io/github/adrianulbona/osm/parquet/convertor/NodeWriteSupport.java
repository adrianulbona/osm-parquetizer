package io.github.adrianulbona.osm.parquet.convertor;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;

import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;


/**
 * Created by adrian.bona on 26/03/16.
 */
public class NodeWriteSupport extends OsmEntityWriteSupport<Node> {

    private final PrimitiveType latType;
    private final PrimitiveType longType;

    public NodeWriteSupport(boolean excludeMetadata) {
        super(excludeMetadata);
        latType = Types.required(DOUBLE).named("latitude");
        longType = Types.required(DOUBLE).named("longitude");
    }

    @Override
    protected MessageType getSchema() {
        final List<Type> attributes = new ArrayList<>(getCommonAttributes());
        attributes.add(latType);
        attributes.add(longType);
        return new MessageType("node", attributes);
    }

    @Override
    protected void writeSpecificFields(Node record, int nextAvailableIndex) {
        recordConsumer.startField(latType.getName(), nextAvailableIndex);
        recordConsumer.addDouble(record.getLatitude());
        recordConsumer.endField(latType.getName(), nextAvailableIndex++);

        recordConsumer.startField(longType.getName(), nextAvailableIndex);
        recordConsumer.addDouble(record.getLongitude());
        recordConsumer.endField(longType.getName(), nextAvailableIndex);
    }
}
