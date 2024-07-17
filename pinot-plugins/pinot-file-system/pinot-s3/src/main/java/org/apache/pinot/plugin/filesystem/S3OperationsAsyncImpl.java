package org.apache.pinot.plugin.filesystem;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
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


/**
 * This implementation uses AWS SDK S3AsyncClient
 */
public class S3OperationsAsyncImpl implements S3Operations {
  private final S3AsyncClient _s3Client;
  private final ExecutorService _uploadPartExecutorService = Executors.newCachedThreadPool();

  public S3OperationsAsyncImpl(S3AsyncClient client) {
    _s3Client = client;
  }

  private static <T> T await(CompletableFuture<T> future) {
    return await(future, 1, TimeUnit.SECONDS);
  }

  private static <T> T await(CompletableFuture<T> future, long n, TimeUnit timeUnit) {
    try {
      return future.get(n, timeUnit);
    } catch (RuntimeException rtException) {
      throw rtException;
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

  @Override
  public HeadObjectResponse headObject(HeadObjectRequest request) {
    return await(_s3Client.headObject(request));
  }

  @Override
  public ListObjectsV2Response listObjectsV2(ListObjectsV2Request listObjectsV2Request) {
    return await(_s3Client.listObjectsV2(listObjectsV2Request));
  }

  @Override
  public CopyObjectResponse copyObject(CopyObjectRequest copyReq) {
    return await(_s3Client.copyObject(copyReq));
  }

  @Override
  public PutObjectResponse putObject(PutObjectRequest request, Path sourceFilePath) {
    return await(_s3Client.putObject(request, sourceFilePath));
  }

  @Override
  public PutObjectResponse putObject(PutObjectRequest request, byte[] sourceFileBytes) {
    return await(_s3Client.putObject(request, AsyncRequestBody.fromBytes(sourceFileBytes)));
  }

  @Override
  public DeleteObjectResponse deleteObject(DeleteObjectRequest deleteObjectRequest) {
    return await(_s3Client.deleteObject(deleteObjectRequest));
  }

  @Override
  public InputStream getObject(GetObjectRequest request) {
    return await(_s3Client.getObject(request, AsyncResponseTransformer.toBlockingInputStream()));
  }

  @Override
  public GetObjectResponse getObject(GetObjectRequest request, File destinationFile) {
    return await(_s3Client.getObject(request, AsyncResponseTransformer.toFile(destinationFile)));
  }

  @Override
  public CreateMultipartUploadResponse createMultipartUpload(CreateMultipartUploadRequest request) {
    return await(_s3Client.createMultipartUpload(request));
  }

  @Override
  public UploadPartResponse uploadPart(UploadPartRequest request, InputStream input, long partSize) {
    return await(_s3Client.uploadPart(request, AsyncRequestBody.fromInputStream(input, partSize, _uploadPartExecutorService)));
  }

  @Override
  public CompleteMultipartUploadResponse completeMultipartUpload(CompleteMultipartUploadRequest request) {
    return await(_s3Client.completeMultipartUpload(request));
  }

  @Override
  public AbortMultipartUploadResponse abortMultipartUpload(AbortMultipartUploadRequest request) {
    return await(_s3Client.abortMultipartUpload(request));
  }

  @Override
  public void close() {
    _s3Client.close();
  }
}
