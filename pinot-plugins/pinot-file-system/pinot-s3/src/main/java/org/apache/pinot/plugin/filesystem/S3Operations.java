package org.apache.pinot.plugin.filesystem;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;


public interface S3Operations {
  HeadObjectResponse headObject(HeadObjectRequest request);

  ListObjectsV2Response listObjectsV2(ListObjectsV2Request listObjectsV2Request);

  CopyObjectResponse copyObject(CopyObjectRequest copyReq);

  PutObjectResponse putObject(PutObjectRequest request, Path sourceFilePath);
  PutObjectResponse putObject(PutObjectRequest request, byte[] sourceFileBytes);

  DeleteObjectResponse deleteObject(DeleteObjectRequest deleteObjectRequest);

  InputStream getObject(GetObjectRequest request);
  GetObjectResponse getObject(GetObjectRequest request, File destinationFile);

  CreateMultipartUploadResponse createMultipartUpload(CreateMultipartUploadRequest request);

  UploadPartResponse uploadPart(UploadPartRequest request, InputStream input, long partSize);

  CompleteMultipartUploadResponse completeMultipartUpload(CompleteMultipartUploadRequest request);

  AbortMultipartUploadResponse abortMultipartUpload(AbortMultipartUploadRequest request);

  CreateBucketResponse createBucket(CreateBucketRequest request);

  void close();

}
