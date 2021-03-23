package io.confluent.connect.storage.partitioner;

import io.confluent.connect.storage.common.StorageCommonConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import java.text.SimpleDateFormat;  
import java.util.Date;  

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

public class IdPartitioner<T> extends DefaultPartitioner<T> {
    private static final Logger log = LoggerFactory.getLogger(IdPartitioner.class);
    private static final String PURPOSE = "Partitioning topic into s3 with key value";

    private List<String> fieldNames;
    private String fileDelim;
    private String fallback;

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, Object> config) {
        fieldNames = (List<String>) config.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
        delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
        fileDelim = (String) config.get(StorageCommonConfig.FILE_DELIM_CONFIG);
        fallback = (String) config.get(PartitionerConfig.ID_PARTITIONER_ERROR_FALLBACK_CONFIG);
    }

    @Override
    public String generatePartitionedPath(String topic, String encodedPartition) {
        return encodedPartition;
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        try {
            return encode(sinkRecord);
        } catch (Exception e) {
            log.warn(
                String.format("IdPartitioner threw an exception: consult the malformed event at '%s%s%s%s%s%s%s.json'",
                    fallback,
                    this.delim,
                    sinkRecord.topic(),
                    fileDelim,
                    sinkRecord.kafkaPartition(),
                    fileDelim,
                    String.format("%010d", sinkRecord.kafkaOffset())),
                e);
            return fallback;
        }
    }

    private String encode(SinkRecord sinkRecord) {
        Map<String, Object> key = requireMap(sinkRecord.key(), PURPOSE);

        StringBuilder builder = new StringBuilder();
        for (String fieldName: fieldNames) {
            if (builder.length() > 0) {
                builder.append(this.delim);
            }
            Object fieldValue = key.get(fieldName);
            builder.append(fieldValue.toString());
        }

        builder.append(this.delim);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd/HH/mm");
        Date date = new Date();
        builder.append(formatter.format(date));

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
