package org.apache.pulsar.io.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.google.common.collect.Lists;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class S3Sink implements Sink<byte[]> {

    private AmazonS3 s3Client;
    private String bucketName;
    private static final Logger LOG = LoggerFactory.getLogger(S3Sink.class);
    private List<Record<byte[]>> buffer;
    private long batchSize;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        final S3SinkConfig cfg = S3SinkConfig.load(config);
        cfg.validate();
        final BasicAWSCredentials awsCredentials = new BasicAWSCredentials(cfg.getAccessKeyId(),
                cfg.getSecretAccessKey());
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withRegion(cfg.getRegion())
                .build();
        bucketName = cfg.getBucketName();
        batchSize = cfg.getBatchSize();
        if (!s3Client.doesBucketExistV2(bucketName)) {
            CreateBucketRequest cbr = new CreateBucketRequest(bucketName, cfg.getRegion());
            cbr.withCannedAcl(CannedAccessControlList.PublicRead);
            s3Client.createBucket(cbr);
        }
        buffer = Lists.newArrayList();
    }

    @Override
    public void write(Record<byte[]> record) throws Exception {
        buffer.add(record);
        if (buffer.size() == batchSize) {
            String objectId = UUID.randomUUID().toString();
            try {
                s3Client.putObject(bucketName,
                        objectId,
                        buffer.stream()
                                .map(r -> new String(r.getValue(), StandardCharsets.UTF_8))
                                .collect(Collectors.joining(",")));
                buffer.forEach(r -> r.ack());
                LOG.info(String.format("Stored %s records with object id %s", batchSize, objectId));
            } catch (Exception e) {
                buffer.forEach(r -> r.fail());
            } finally {
                buffer = Lists.newArrayList();
            }
        }
    }

    @Override
    public void close() throws Exception {

    }
}
