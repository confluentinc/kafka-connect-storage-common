package io.confluent.connect.storage.partitioner;

import io.confluent.connect.storage.common.StorageCommonConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

public class IdPartitioner<T> extends DefaultPartitioner<T> {
    private static final Logger log = LoggerFactory.getLogger(IdPartitioner.class);
    private List<String> fieldNames;
    private static final String PURPOSE = "Partitioning topic into s3 with key value";

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, Object> config) {
        fieldNames = (List<String>) config.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
        delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    }

    @Override
    public String generatePartitionedPath(String topic, String encodedPartition) {
        return encodedPartition;
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        log.info(value.toString());

        Map<String, Object> key = requireMap(sinkRecord.value(), PURPOSE);
        log.info(key.toString());

        StringBuilder builder = new StringBuilder();
        for (String fieldName: fieldNames) {
            if (builder.length() > 0) {
                builder.append(this.delim);
            }
            log.info(fieldName);
            Object fieldValue = key.get(fieldName);
            log.info(fieldValue.toString());
            builder.append(fieldValue.toString());
            log.info(fieldValue.getClass().toString());
        }

        return builder.toString();
    }

    @Override
    public List<T> partitionFields() {
        if (partitionFields == null) {
            partitionFields = newSchemaGenerator(config).newPartitionFields(
                    Utils.join(fieldNames, ",")
            );
        }
        return partitionFields;
    }
}
