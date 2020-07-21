package org.apache.pulsar.io.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Configuration class for the S3 sink.
 */
@Data
@Accessors(chain = true)
public class S3SinkConfig {

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "AWS Access Key ID that has S3 permissions")
    private String accessKeyId;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "AWS Secret Access Key")
    private String secretAccessKey;

    @FieldDoc(
            required = true,
            defaultValue = "us-east-1",
            help = "AWS Region")
    private String region;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "AWS S3 Bucket name")
    private String bucketName;

    @FieldDoc(
            defaultValue = "100L",
            help = "Number of records the S3 sink will try to batch together before sending to S3")
    private long batchSize = 100L;


    public static S3SinkConfig load(String yamlFile) throws IOException {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final S3SinkConfig cfg = mapper.readValue(new File(yamlFile), S3SinkConfig.class);
        return cfg;
    }

    public static S3SinkConfig load(Map<String, Object> map) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final S3SinkConfig cfg = mapper.readValue(new ObjectMapper().writeValueAsString(map),
                S3SinkConfig.class);
        return cfg;
    }

    public void validate() {
        if (StringUtils.isEmpty(getAccessKeyId()) || StringUtils.isEmpty(getSecretAccessKey()) ||
        StringUtils.isEmpty(getRegion()) || StringUtils.isEmpty(getBucketName())) {
            throw new IllegalArgumentException("Required property not set.");
        }
    }
}
