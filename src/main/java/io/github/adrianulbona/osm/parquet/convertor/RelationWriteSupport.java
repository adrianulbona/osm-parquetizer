package io.github.adrianulbona.osm.parquet.convertor;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;

import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;


/**
 * Created by adrian.bona on 26/03/16.
 */
public class RelationWriteSupport extends OsmEntityWriteSupport<Relation> {

    private final PrimitiveType membersType;

    public RelationWriteSupport() {
        membersType = new PrimitiveType(REPEATED, INT64, "members");
    }

    @Override
    protected MessageType getSchema() {
        final List<Type> attributes = new ArrayList<>(getCommonAttributes());
        attributes.add(membersType);
        return new MessageType("relation", attributes);
    }

    @Override
    protected void writeSpecificFields(Relation record, int nextAvailableIndex) {
        if (!record.getMembers().isEmpty()) {
            recordConsumer.startField(membersType.getName(), nextAvailableIndex);
            record.getMembers().forEach(member -> recordConsumer.addLong(member.getMemberId()));
            recordConsumer.endField(membersType.getName(), nextAvailableIndex);
        }
    }
}
