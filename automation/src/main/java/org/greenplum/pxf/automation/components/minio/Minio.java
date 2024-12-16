package org.greenplum.pxf.automation.components.minio;

import io.qameta.allure.Step;
import org.greenplum.pxf.automation.components.common.BaseSystemObject;

import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Minio System Object
 */
public class Minio extends BaseSystemObject {

    private S3Client s3Client;
    private String accessKeyId;
    private String secretKey;

    public Minio() {
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretKey() {
        return secretKey;
    }

    @Override
    public void init() throws Exception {
        super.init();
        String endpoint = this.getProperty("endpoint");
        accessKeyId = this.getProperty("accessKeyId");
        secretKey = this.getProperty("secretKey");
        s3Client = getS3Client(endpoint, accessKeyId, secretKey);
    }

    private static S3Client getS3Client(String endpoint, String accessKeyId, String secretKey) {
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_WEST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretKey)))
                .forcePathStyle(true)
                .build();
    }

    @Step("Create S3 bucket")
    public void createBucket(String bucketName) {
        if (!isBucketExists(bucketName)) {
            try {
                CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                        .bucket(bucketName)
                        .build();
                s3Client.createBucket(bucketRequest);
                logMessage(String.format("Create bucket %s", bucketName));
            } catch (Exception e) {
                throw new RuntimeException("Failed to check or create S3 bucket: " + e.getMessage(), e);
            }
        }
    }

    @Step("Upload file to S3 bucket")
    public void uploadFile(String bucketName, String key, Path path) {
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            s3Client.putObject(putObjectRequest, path);
            logMessage(String.format("Upload the file %s to the bucket %s with the path %s", path.getFileName(), bucketName, key));
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload file to S3 bucket: " + e.getMessage(), e);
        }
    }

    @Step("Clean S3 bucket")
    public void clean(String bucketName) {
        if (isBucketExists(bucketName)) {
            try {
                ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .build();
                ListObjectsV2Iterable paginatedListResponse = s3Client.listObjectsV2Paginator(listRequest);

                for (ListObjectsV2Response listResponse : paginatedListResponse) {
                    List<ObjectIdentifier> objects = listResponse.contents().stream()
                            .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                            .collect(Collectors.toList());
                    if (objects.isEmpty()) {
                        break;
                    }
                    DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
                            .bucket(bucketName)
                            .delete(Delete.builder().objects(objects).build())
                            .build();
                    s3Client.deleteObjects(deleteRequest);
                }
                deleteBucket(bucketName);
            } catch (Exception e) {
                throw new RuntimeException("Failed to delete objects from S3 bucket: " + e.getMessage(), e);
            }
        }
    }

    @Step("Delete S3 bucket")
    private void deleteBucket(String bucketName) {
        try {
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            s3Client.deleteBucket(deleteBucketRequest);
            logMessage(String.format("Delete bucket %s", bucketName));
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete S3 bucket: " + e.getMessage(), e);
        }
    }

    private boolean isBucketExists(String bucketName) {
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();
        try {
            s3Client.headBucket(headBucketRequest);
            logMessage(String.format("Bucket %s already exists", bucketName));
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }

    private void logMessage(String message) {
        ReportUtils.report(report, getClass(), message);
    }
}
