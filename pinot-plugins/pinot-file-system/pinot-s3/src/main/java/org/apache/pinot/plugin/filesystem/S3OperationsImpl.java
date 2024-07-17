package org.apache.pinot.plugin.filesystem;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
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


public class S3OperationsImpl implements S3Operations {
  private final S3Client _s3Client;

  public S3OperationsImpl(S3Client s3Client) {
    _s3Client = s3Client;
  }

  @Override
  public HeadObjectResponse headObject(HeadObjectRequest request) {
    return _s3Client.headObject(request);
  }

  @Override
  public ListObjectsV2Response listObjectsV2(ListObjectsV2Request listObjectsV2Request) {
    return _s3Client.listObjectsV2(listObjectsV2Request);
  }

  @Override
  public CopyObjectResponse copyObject(CopyObjectRequest copyReq) {
    return _s3Client.copyObject(copyReq);
  }

  @Override
  public PutObjectResponse putObject(PutObjectRequest putObjectRequest, Path sourceFilePath) {
    return _s3Client.putObject(putObjectRequest, RequestBody.fromFile(sourceFilePath));
  }

  @Override
  public PutObjectResponse putObject(PutObjectRequest putObjectRequest, byte[] sourceFileBytes) {
    return _s3Client.putObject(putObjectRequest, RequestBody.fromBytes(sourceFileBytes));
  }

  @Override
  public DeleteObjectResponse deleteObject(DeleteObjectRequest deleteObjectRequest) {
    return _s3Client.deleteObject(deleteObjectRequest);
  }

  @Override
  public InputStream getObject(GetObjectRequest getObjectRequest) {
    return _s3Client.getObject(getObjectRequest);
  }

  @Override
  public GetObjectResponse getObject(GetObjectRequest request, File destinationFile) {
    return _s3Client.getObject(request, destinationFile.toPath());
  }

  @Override
  public CreateMultipartUploadResponse createMultipartUpload(CreateMultipartUploadRequest request) {
    return _s3Client.createMultipartUpload(request);
  }

  @Override
  public UploadPartResponse uploadPart(UploadPartRequest request, InputStream input, long partSize) {
    return _s3Client.uploadPart(request, RequestBody.fromInputStream(input, partSize));
  }

  @Override
  public CompleteMultipartUploadResponse completeMultipartUpload(CompleteMultipartUploadRequest request) {
    return _s3Client.completeMultipartUpload(request);
  }

  @Override
  public AbortMultipartUploadResponse abortMultipartUpload(AbortMultipartUploadRequest request) {
    return _s3Client.abortMultipartUpload(request);
  }

  @Override
  public void close() {
    _s3Client.close();
  }
}
