package io.github.adrianulbona.osm.parquet.convertor;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;

import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;


/**
 * Created by adrian.bona on 26/03/16.
 */
public class RelationWriteSupport extends OsmEntityWriteSupport<Relation> {

    private final GroupType membersType;
    private final PrimitiveType memberIdType;
    private final PrimitiveType memberRoleType;
    private final PrimitiveType memberTypeType;

    public RelationWriteSupport(boolean excludeMetadata) {
        super(excludeMetadata);
        memberIdType = new PrimitiveType(REQUIRED, INT64, "id");
        memberRoleType = new PrimitiveType(REQUIRED, BINARY, "role");
        memberTypeType = new PrimitiveType(REQUIRED, BINARY, "type");
        membersType = new GroupType(REPEATED, "members", memberIdType, memberRoleType, memberTypeType);
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
            record.getMembers().forEach(member -> {
                recordConsumer.startGroup();

                recordConsumer.startField(memberIdType.getName(), 0);
                recordConsumer.addLong(member.getMemberId());
                recordConsumer.endField(memberIdType.getName(), 0);

                recordConsumer.startField(memberRoleType.getName(), 1);
                recordConsumer.addBinary(Binary.fromString(member.getMemberRole()));
                recordConsumer.endField(memberRoleType.getName(), 1);

                recordConsumer.startField(memberTypeType.getName(), 2);
                recordConsumer.addBinary(Binary.fromString(member.getMemberType().name()));
                recordConsumer.endField(memberTypeType.getName(), 2);

                recordConsumer.endGroup();
            });
            recordConsumer.endField(membersType.getName(), nextAvailableIndex);
        }
    }
}
