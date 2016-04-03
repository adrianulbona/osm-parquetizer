package io.github.adrianulbona.osm.parquet.convertor;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.*;


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

    protected RecordConsumer recordConsumer;

    public OsmEntityWriteSupport() {
        idType = new PrimitiveType(REQUIRED, INT64, "id");
        tagKeyType = new PrimitiveType(REQUIRED, BINARY, "key");
        tagValueType = new PrimitiveType(OPTIONAL, BINARY, "value");
        tags = new GroupType(REPEATED, "tags", tagKeyType, tagValueType);
        versionType = new PrimitiveType(OPTIONAL, INT32, "version");
        timestampType = new PrimitiveType(OPTIONAL, INT64, "timestamp");
        changesetType = new PrimitiveType(OPTIONAL, INT64, "changeset");
        uidType = new PrimitiveType(OPTIONAL, INT32, "uid");
        userSidType = new PrimitiveType(OPTIONAL, BINARY, "user_sid");
    }

    protected List<Type> getCommonAttributes() {
        return asList(idType, versionType, timestampType, changesetType, uidType, userSidType, tags);
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
