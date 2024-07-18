/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.filesystem;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.Reporter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;


@Test
@Listeners(S3PinotFSTest.SimpleListener.class)
public class S3PinotFSTest implements IInvokedMethodListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3PinotFSTest.class);
  private static final String S3MOCK_VERSION = System.getProperty("s3mock.version", "2.12.2");
  private static S3MockContainer S3_MOCK_CONTAINER;
  private static final String DELIMITER = "/";
  private static final String SCHEME = "s3";
  private static final String FILE_FORMAT = "%s://%s/%s";
  private static final String DIR_FORMAT = "%s://%s";

  private S3PinotFS _s3PinotFS;
  private S3Operations _s3Operations;
  private String _bucket;
  private File TEMP_FILE;

  public S3PinotFSTest(final S3Operations s3Ops, final String bucketName) {
    LOGGER.info("S3Operations class: " + s3Ops.getClass().getName());
    this._s3Operations = s3Ops;
    this._bucket = bucketName;
    TEMP_FILE = new File(FileUtils.getTempDirectory(), "S3PinotFSTest-" + System.currentTimeMillis());
    System.out.println("TEMP_FILE: " + TEMP_FILE);
  }

  @Factory
  public static Object[] createTestInstances() {
    S3_MOCK_CONTAINER = new S3MockContainer(S3MOCK_VERSION);
    S3_MOCK_CONTAINER.start();
    String endpoint = S3_MOCK_CONTAINER.getHttpEndpoint();
    S3Client s3Client = createS3ClientV2(endpoint);
    S3AsyncClient crtClient = createS3CrtClientV2(endpoint);
    S3AsyncClient nettyClient = createS3NettyClientV2(endpoint);

    return new Object[] {
        new S3PinotFSTest(new S3OperationsImpl(s3Client), "test-bucket-s3client"),
        new S3PinotFSTest(new S3OperationsAsyncImpl(crtClient), "test-bucket-crtclient"),
        new S3PinotFSTest(new S3OperationsAsyncImpl(nettyClient), "test-bucket-nettyclient")
    };
  }

  @BeforeClass
  public void setUp() {
    _s3PinotFS = new S3PinotFS();
    _s3PinotFS.init(_s3Operations);
    _s3Operations.createBucket(CreateBucketRequest.builder().bucket(_bucket).build());
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _s3PinotFS.close();
    _s3Operations.close();
    // S3_MOCK_CONTAINER.stop();
    FileUtils.deleteQuietly(TEMP_FILE);
  }

  private void createEmptyFile(String folderName, String fileName) {
    String fileNameWithFolder = folderName.length() == 0 ? fileName : folderName + DELIMITER + fileName;
    _s3Operations.putObject(S3TestUtils.getPutObjectRequest(_bucket, fileNameWithFolder), new byte[0]);
  }

  @Test
  public void testTouchFileInBucket()
      throws Exception {

    String[] originalFiles = new String[]{"a-touch.txt", "b-touch.txt", "c-touch.txt"};

    for (String fileName : originalFiles) {
      _s3PinotFS.touch(URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, fileName)));
    }
    ListObjectsV2Response listObjectsV2Response =
        _s3Operations.listObjectsV2(S3TestUtils.getListObjectRequest(_bucket, "", true));

    String[] response = listObjectsV2Response.contents().stream().map(S3Object::key).filter(x -> x.contains("touch"))
        .toArray(String[]::new);

    Assert.assertEquals(response.length, originalFiles.length);
    Assert.assertTrue(Arrays.equals(response, originalFiles));
  }

  @Test
  public void testTouchFilesInFolder()
      throws Exception {

    String folder = "my-files";
    String[] originalFiles = new String[]{"a-touch.txt", "b-touch.txt", "c-touch.txt"};

    for (String fileName : originalFiles) {
      String fileNameWithFolder = folder + DELIMITER + fileName;
      _s3PinotFS.touch(URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, fileNameWithFolder)));
    }
    ListObjectsV2Response listObjectsV2Response =
        _s3Operations.listObjectsV2(S3TestUtils.getListObjectRequest(_bucket, folder, false));

    String[] response = listObjectsV2Response.contents().stream().map(S3Object::key).filter(x -> x.contains("touch"))
        .toArray(String[]::new);
    Assert.assertEquals(response.length, originalFiles.length);

    Assert.assertTrue(Arrays.equals(response, Arrays.stream(originalFiles).map(x -> folder + DELIMITER + x).toArray()));
  }

  @Test
  public void testListFilesInBucketNonRecursive()
      throws Exception {
    String[] originalFiles = new String[]{"a-list.txt", "b-list.txt", "c-list.txt"};
    List<String> expectedFileNames = new ArrayList<>();

    for (String fileName : originalFiles) {
      createEmptyFile("", fileName);
      expectedFileNames.add(String.format(FILE_FORMAT, SCHEME, _bucket, fileName));
    }

    String[] actualFiles = _s3PinotFS.listFiles(URI.create(String.format(DIR_FORMAT, SCHEME, _bucket)), false);

    actualFiles = Arrays.stream(actualFiles).filter(x -> x.contains("list")).toArray(String[]::new);
    Assert.assertEquals(actualFiles.length, originalFiles.length);

    Assert.assertTrue(Arrays.equals(actualFiles, expectedFileNames.toArray()));
  }

  @Test
  public void testListFilesInFolderNonRecursive()
      throws Exception {
    String folder = "list-files";
    String[] originalFiles = new String[]{"a-list-2.txt", "b-list-2.txt", "c-list-2.txt"};

    for (String fileName : originalFiles) {
      createEmptyFile(folder, fileName);
    }
    // Files in sub folders should be skipped.
    createEmptyFile(folder + DELIMITER + "subfolder1", "a-sub-file.txt");
    createEmptyFile(folder + DELIMITER + "subfolder2", "a-sub-file.txt");

    String[] actualFiles = _s3PinotFS.listFiles(URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, folder)), false);
    Assert.assertEquals(actualFiles.length, originalFiles.length);

    actualFiles = Arrays.stream(actualFiles).filter(x -> x.contains("list-2")).toArray(String[]::new);
    Assert.assertEquals(actualFiles.length, originalFiles.length);

    Assert.assertTrue(Arrays.equals(Arrays.stream(originalFiles)
            .map(fileName -> String.format(FILE_FORMAT, SCHEME, _bucket, folder + DELIMITER + fileName)).toArray(),
        actualFiles));
  }

  @Test
  public void testListFilesInFolderRecursive()
      throws Exception {
    String folder = "list-files-rec";
    String[] nestedFolders = new String[]{"", "list-files-child-1", "list-files-child-2"};
    String[] originalFiles = new String[]{"a-list-3.txt", "b-list-3.txt", "c-list-3.txt"};

    List<String> expectedResultList = new ArrayList<>();
    for (String childFolder : nestedFolders) {
      String folderName = folder + (childFolder.length() == 0 ? "" : DELIMITER + childFolder);
      for (String fileName : originalFiles) {
        createEmptyFile(folderName, fileName);
        expectedResultList.add(String.format(FILE_FORMAT, SCHEME, _bucket, folderName + DELIMITER + fileName));
      }
    }
    String[] actualFiles = _s3PinotFS.listFiles(URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, folder)), true);
    Assert.assertEquals(actualFiles.length, expectedResultList.size());
    actualFiles = Arrays.stream(actualFiles).filter(x -> x.contains("list-3")).toArray(String[]::new);
    Assert.assertEquals(actualFiles.length, expectedResultList.size());
    Assert.assertTrue(Arrays.equals(expectedResultList.toArray(), actualFiles));
  }

  @Test
  public void testListFilesWithMetadataInFolderNonRecursive()
      throws Exception {
    String folder = "list-files-with-md";
    String[] originalFiles = new String[]{"a-list-2.txt", "b-list-2.txt", "c-list-2.txt"};
    for (String fileName : originalFiles) {
      createEmptyFile(folder, fileName);
    }
    // Files in sub folders should be skipped.
    createEmptyFile(folder + DELIMITER + "subfolder1", "a-sub-file.txt");
    createEmptyFile(folder + DELIMITER + "subfolder2", "a-sub-file.txt");
    List<FileMetadata> actualFiles =
        _s3PinotFS.listFilesWithMetadata(URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, folder)), false);
    Assert.assertEquals(actualFiles.size(), originalFiles.length);
    List<String> actualFilePaths =
        actualFiles.stream().map(FileMetadata::getFilePath).filter(fp -> fp.contains("list-2"))
            .collect(Collectors.toList());
    Assert.assertEquals(actualFilePaths.size(), originalFiles.length);
    Assert.assertEquals(Arrays.stream(originalFiles)
        .map(fileName -> String.format(FILE_FORMAT, SCHEME, _bucket, folder + DELIMITER + fileName))
        .collect(Collectors.toList()), actualFilePaths);
  }

  @Test
  public void testListFilesWithMetadataInFolderRecursive()
      throws Exception {
    String folder = "list-files-rec-with-md";
    String[] nestedFolders = new String[]{"", "list-files-child-1", "list-files-child-2"};
    String[] originalFiles = new String[]{"a-list-3.txt", "b-list-3.txt", "c-list-3.txt"};

    List<String> expectedResultList = new ArrayList<>();
    for (String childFolder : nestedFolders) {
      String folderName = folder + (childFolder.length() == 0 ? "" : DELIMITER + childFolder);
      for (String fileName : originalFiles) {
        createEmptyFile(folderName, fileName);
        expectedResultList.add(String.format(FILE_FORMAT, SCHEME, _bucket, folderName + DELIMITER + fileName));
      }
    }
    List<FileMetadata> actualFiles =
        _s3PinotFS.listFilesWithMetadata(URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, folder)), true);
    Assert.assertEquals(actualFiles.size(), expectedResultList.size());
    List<String> actualFilePaths =
        actualFiles.stream().map(FileMetadata::getFilePath).filter(fp -> fp.contains("list-3"))
            .collect(Collectors.toList());
    Assert.assertEquals(actualFilePaths.size(), expectedResultList.size());
    Assert.assertEquals(expectedResultList, actualFilePaths);
  }

  @Test
  public void testDeleteFile()
      throws Exception {
    String[] originalFiles = new String[]{"a-delete.txt", "b-delete.txt", "c-delete.txt"};
    String fileToDelete = "a-delete.txt";

    List<String> expectedResultList = new ArrayList<>();
    for (String fileName : originalFiles) {
      createEmptyFile("", fileName);
      if (!fileName.equals(fileToDelete)) {
        expectedResultList.add(fileName);
      }
    }

    boolean deleteResult = _s3PinotFS.delete(
        URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, fileToDelete)), false);

    Assert.assertTrue(deleteResult);

    ListObjectsV2Response listObjectsV2Response =
        _s3Operations.listObjectsV2(S3TestUtils.getListObjectRequest(_bucket, "", true));
    String[] actualResponse =
        listObjectsV2Response.contents().stream().map(S3Object::key).filter(x -> x.contains("delete"))
            .toArray(String[]::new);

    Assert.assertEquals(actualResponse.length, 2);
    Assert.assertTrue(Arrays.equals(actualResponse, expectedResultList.toArray()));
  }

  @Test
  public void testDeleteFolder()
      throws Exception {
    String[] originalFiles = new String[]{"a-delete-2.txt", "b-delete-2.txt", "c-delete-2.txt"};
    String folderName = "my-files";

    for (String fileName : originalFiles) {
      createEmptyFile(folderName, fileName);
    }

    boolean deleteResult = _s3PinotFS.delete(
        URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, folderName)), true);

    Assert.assertTrue(deleteResult);

    ListObjectsV2Response listObjectsV2Response =
        _s3Operations.listObjectsV2(S3TestUtils.getListObjectRequest(_bucket, "", true));
    String[] actualResponse =
        listObjectsV2Response.contents().stream().map(S3Object::key).filter(x -> x.contains("delete-2"))
            .toArray(String[]::new);

    Assert.assertEquals(0, actualResponse.length);
  }

  @Test
  public void testIsDirectory()
      throws Exception {
    String[] originalFiles = new String[]{"a-dir.txt", "b-dir.txt", "c-dir.txt"};
    String folder = "my-files-dir";
    String childFolder = "my-files-dir-child";
    for (String fileName : originalFiles) {
      String folderName = folder + DELIMITER + childFolder;
      createEmptyFile(folderName, fileName);
    }

    boolean isBucketDir = _s3PinotFS.isDirectory(URI.create(String.format(DIR_FORMAT, SCHEME, _bucket)));
    boolean isDir = _s3PinotFS.isDirectory(URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, folder)));
    boolean isDirChild = _s3PinotFS.isDirectory(
        URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, folder + DELIMITER + childFolder)));
    boolean notIsDir = _s3PinotFS.isDirectory(URI.create(
        String.format(FILE_FORMAT, SCHEME, _bucket, folder + DELIMITER + childFolder + DELIMITER + "a-delete.txt")));

    Assert.assertTrue(isBucketDir);
    Assert.assertTrue(isDir);
    Assert.assertTrue(isDirChild);
    Assert.assertFalse(notIsDir);
  }

  @Test
  public void testExists()
      throws Exception {
    String[] originalFiles = new String[]{"a-ex.txt", "b-ex.txt", "c-ex.txt"};
    String folder = "my-files-dir";
    String childFolder = "my-files-dir-child";

    for (String fileName : originalFiles) {
      String folderName = folder + DELIMITER + childFolder;
      createEmptyFile(folderName, fileName);
    }

    boolean bucketExists = _s3PinotFS.exists(URI.create(String.format(DIR_FORMAT, SCHEME, _bucket)));
    boolean dirExists = _s3PinotFS.exists(URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, folder)));
    boolean childDirExists =
        _s3PinotFS.exists(URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, folder + DELIMITER + childFolder)));
    boolean fileExists = _s3PinotFS.exists(URI.create(
        String.format(FILE_FORMAT, SCHEME, _bucket, folder + DELIMITER + childFolder + DELIMITER + "a-ex.txt")));
    boolean fileNotExists = _s3PinotFS.exists(URI.create(
        String.format(FILE_FORMAT, SCHEME, _bucket, folder + DELIMITER + childFolder + DELIMITER + "d-ex.txt")));

    Assert.assertTrue(bucketExists);
    Assert.assertTrue(dirExists);
    Assert.assertTrue(childDirExists);
    Assert.assertTrue(fileExists);
    Assert.assertFalse(fileNotExists);
  }

  @Test
  public void testCopyFromAndToLocal()
      throws Exception {
    String fileName = "copyFile-" + System.currentTimeMillis() + ".txt";
    File fileToCopy = new File(TEMP_FILE, fileName);
    File fileToDownload = new File(TEMP_FILE, "copyFile_download-" + System.currentTimeMillis() + ".txt").getAbsoluteFile();
    try {
      createDummyFile(fileToCopy, 1024);
      System.out.println("fileToCopy: " + fileToCopy + " size=" +fileToCopy.length());
      _s3PinotFS.copyFromLocalFile(fileToCopy, URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, fileName)));
      HeadObjectResponse headObjectResponse = _s3Operations.headObject(S3TestUtils.getHeadObjectRequest(_bucket, fileName));
      Assert.assertEquals(headObjectResponse.contentLength(), (Long) fileToCopy.length());
      _s3PinotFS.copyToLocalFile(URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, fileName)), fileToDownload);
      Assert.assertEquals(fileToCopy.length(), fileToDownload.length());
    } finally {
      FileUtils.deleteQuietly(fileToCopy);
      FileUtils.deleteQuietly(fileToDownload);
    }
  }

  @Test
  public void testMultiPartUpload()
      throws Exception {
    String fileName = "copyFile_for_multipart.txt";
    File fileToCopy = new File(TEMP_FILE, fileName);
    File fileToDownload = new File(TEMP_FILE, "copyFile_download_multipart.txt").getAbsoluteFile();
    try {
      // Make a file of 11MB to upload in parts, whose min required size is 5MB.
      createDummyFile(fileToCopy, 11 * 1024 * 1024);
      System.out.println("fileToCopy.length:" + fileToCopy.length());
      _s3PinotFS.setMultiPartUploadConfigs(1, 5 * 1024 * 1024);
      try {
        _s3PinotFS.copyFromLocalFile(fileToCopy, URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, fileName)));
      } finally {
        // disable multipart upload again for the other UT cases.
        _s3PinotFS.setMultiPartUploadConfigs(-1, 128 * 1024 * 1024);
      }
      HeadObjectResponse headObjectResponse = _s3Operations.headObject(S3TestUtils.getHeadObjectRequest(_bucket, fileName));
      Assert.assertEquals(headObjectResponse.contentLength(), (Long) fileToCopy.length());
      _s3PinotFS.copyToLocalFile(URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, fileName)), fileToDownload);
      Assert.assertEquals(fileToCopy.length(), fileToDownload.length());
    } finally {
      FileUtils.deleteQuietly(fileToCopy);
      FileUtils.deleteQuietly(fileToDownload);
    }
  }

  @Test
  public void testOpenFile()
      throws Exception {
    System.out.println("s3Ops: " + _s3Operations);

    String fileName = "sample-" + System.currentTimeMillis() + ".txt";
    String fileContent = "Hello, World";

    _s3Operations.putObject(S3TestUtils.getPutObjectRequest(_bucket, fileName), fileContent.getBytes(StandardCharsets.UTF_8));

    InputStream is = _s3PinotFS.open(URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, fileName)));
    String actualContents = IOUtils.toString(is, StandardCharsets.UTF_8);
    Assert.assertEquals(actualContents, fileContent);
  }

  @Test
  public void testMkdir()
      throws Exception {
    String folderName = "my-test-folder";

    _s3PinotFS.mkdir(URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, folderName)));

    HeadObjectResponse headObjectResponse =
        _s3Operations.headObject(S3TestUtils.getHeadObjectRequest(_bucket, folderName + "/"));
    Assert.assertTrue(headObjectResponse.sdkHttpResponse().isSuccessful());
  }

  @Test
  public void testMoveFile()
      throws Exception {

    String fileName = "file-to-move";
    int fileSize = 5000;

    File file = new File(TEMP_FILE, fileName);

    try {
      createDummyFile(file, fileSize);
      URI sourceUri = URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, fileName));

      _s3PinotFS.copyFromLocalFile(file, sourceUri);

      HeadObjectResponse sourceHeadObjectResponse =
          _s3Operations.headObject(S3TestUtils.getHeadObjectRequest(_bucket, fileName));

      URI targetUri = URI.create(String.format(FILE_FORMAT, SCHEME, _bucket, "move-target"));

      boolean moveResult = _s3PinotFS.move(sourceUri, targetUri, false);
      Assert.assertTrue(moveResult);

      Assert.assertFalse(_s3PinotFS.exists(sourceUri));
      Assert.assertTrue(_s3PinotFS.exists(targetUri));

      HeadObjectResponse targetHeadObjectResponse =
          _s3Operations.headObject(S3TestUtils.getHeadObjectRequest(_bucket, "move-target"));
      Assert.assertEquals(targetHeadObjectResponse.contentLength(),
          fileSize);
      Assert.assertEquals(targetHeadObjectResponse.storageClass(),
          sourceHeadObjectResponse.storageClass());
      Assert.assertEquals(targetHeadObjectResponse.archiveStatus(),
          sourceHeadObjectResponse.archiveStatus());
      Assert.assertEquals(targetHeadObjectResponse.contentType(),
          sourceHeadObjectResponse.contentType());
      Assert.assertEquals(targetHeadObjectResponse.expiresString(),
          sourceHeadObjectResponse.expiresString());
      Assert.assertEquals(targetHeadObjectResponse.eTag(),
          sourceHeadObjectResponse.eTag());
      Assert.assertEquals(targetHeadObjectResponse.replicationStatusAsString(),
          sourceHeadObjectResponse.replicationStatusAsString());
      // Last modified time might not be exactly the same, hence we give 5 seconds buffer.
      Assert.assertTrue(Math.abs(
          targetHeadObjectResponse.lastModified().getEpochSecond() - sourceHeadObjectResponse.lastModified()
              .getEpochSecond()) < 5);
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  private static void createDummyFile(File file, int size)
      throws IOException {
    FileUtils.deleteQuietly(file);
    FileUtils.touch(file);
    try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
      for (int i = 0; i < size; i++) {
        out.write((byte) i);
      }
    }
  }

  private static S3Client createS3ClientV2(String endpoint) {
    return S3Client.builder().region(Region.of("us-east-1"))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        .endpointOverride(URI.create(endpoint)).build();
  }

  private static S3AsyncClient createS3CrtClientV2(String endpoint) {
    return S3AsyncClient.crtBuilder()
        .region(Region.of("us-east-1"))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
        .forcePathStyle(true)
        .endpointOverride(URI.create(endpoint))
        .build();
  }

  private static S3AsyncClient createS3NettyClientV2(String endpoint) {
    return S3AsyncClient.builder()
        .httpClientBuilder(NettyNioAsyncHttpClient.builder())
        .region(Region.of("us-east-1"))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
        .forcePathStyle(true)
        .endpointOverride(URI.create(endpoint))
        .build();
  }

  public static class SimpleListener implements IInvokedMethodListener {
    public SimpleListener() { }

    @Override
    public void beforeInvocation(IInvokedMethod method, ITestResult testResult, ITestContext context) {
      if (method.isTestMethod()) {
        S3PinotFSTest test = (S3PinotFSTest) method.getTestMethod().getInstance();
        ITestNGMethod testMethod = method.getTestMethod();
        Reporter.log(testMethod.getMethodName()
            + " "
            + test._s3Operations.toString());
      }
    }

  }
}
