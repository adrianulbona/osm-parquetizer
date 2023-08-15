package io.github.adrianulbona.osm.parquet.convertor;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.*;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.parquet.schema.LogicalTypeAnnotation.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;


/**
 * Created by adrian.bona on 26/03/16.
 */
public abstract class OsmEntityWriteSupport<E extends Entity> extends WriteSupport<E> {

    private final PrimitiveType idType;
    private final PrimitiveType versionType;
    private final GroupType tags;
    private final PrimitiveType tagKeyType;
    private final PrimitiveType tagValueType;
    private final PrimitiveType timestampType;
    private final PrimitiveType changesetType;
    private final PrimitiveType uidType;
    private final PrimitiveType userSidType;

    private final boolean excludeMetadata;

    protected RecordConsumer recordConsumer;

    public OsmEntityWriteSupport(boolean excludeMetadata) {
        idType = Types.required(INT64).named("id");
        tagKeyType = Types.required(BINARY).as(stringType()).named("key");
        tagValueType = Types.optional(BINARY).as(stringType()).named("value");
        tags = Types.repeatedGroup().addFields(tagKeyType, tagValueType).named("tags");
        versionType = Types.optional(INT32).named("version");
        timestampType = Types.optional(INT64).named("timestamp");
        changesetType = Types.optional(INT64).named("changeset");
        uidType = Types.optional(INT32).named("uid");
        userSidType = Types.optional(BINARY).as(stringType()).named("user_sid");
        this.excludeMetadata = excludeMetadata;
    }

    protected List<Type> getCommonAttributes() {
        final List<Type> commonAttributes = new LinkedList<>();
        commonAttributes.add(idType);
        if (!excludeMetadata) {
            commonAttributes.addAll(asList(versionType, timestampType, changesetType, uidType, userSidType));
        }
        commonAttributes.add(tags);
        return commonAttributes;
    }

    @Override
    public WriteContext init(Configuration config) {
        return new WriteContext(getSchema(), Collections.emptyMap());
    }

    protected abstract MessageType getSchema();

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    protected abstract void writeSpecificFields(E record, int nextAvailableIndex);

    public void write(E record) {
        int index = 0;
        recordConsumer.startMessage();
        recordConsumer.startField(idType.getName(), index);
        recordConsumer.addLong(record.getId());
        recordConsumer.endField(idType.getName(), index++);

        if (!excludeMetadata) {
            recordConsumer.startField(versionType.getName(), index);
            recordConsumer.addInteger(record.getVersion());
            recordConsumer.endField(versionType.getName(), index++);

            recordConsumer.startField(timestampType.getName(), index);
            recordConsumer.addLong(record.getTimestamp().getTime());
            recordConsumer.endField(timestampType.getName(), index++);

            recordConsumer.startField(changesetType.getName(), index);
            recordConsumer.addLong(record.getChangesetId());
            recordConsumer.endField(changesetType.getName(), index++);

            recordConsumer.startField(uidType.getName(), index);
            recordConsumer.addInteger(record.getUser().getId());
            recordConsumer.endField(uidType.getName(), index++);

            recordConsumer.startField(userSidType.getName(), index);
            recordConsumer.addBinary(Binary.fromString(record.getUser().getName()));
            recordConsumer.endField(userSidType.getName(), index++);
        }

        if (!record.getTags().isEmpty()) {
            recordConsumer.startField(tags.getName(), index);
            for (Tag tag : record.getTags()) {
                recordConsumer.startGroup();

                recordConsumer.startField(tagKeyType.getName(), 0);
                recordConsumer.addBinary(Binary.fromString(tag.getKey()));
                recordConsumer.endField(tagKeyType.getName(), 0);

                recordConsumer.startField(tagValueType.getName(), 1);
                recordConsumer.addBinary(Binary.fromString(tag.getValue()));
                recordConsumer.endField(tagValueType.getName(), 1);

                recordConsumer.endGroup();
            }
            recordConsumer.endField(tags.getName(), index);
        }
        index++;

        writeSpecificFields(record, index);
        recordConsumer.endMessage();
    }
}
