package com.xiaomi.infra.galaxy.fds.source;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Migration source for AWS S3
 */
public class S3Source implements Source {
  private static final Log LOG = LogFactory.getLog(S3Source.class);
  public static final String S3_ENDPOINT_NAME = "s3.endpoint.name";
  public static final String S3_BUCKET_NAME = "s3.bucket.name";
  public static final String S3_ACCESS_KEY = "s3.access.key";
  public static final String S3_ACCESS_SECRET = "s3.access.secret";

  private AmazonS3Client client;
  private String bucketName;
  private AccessControlList acls;
  private boolean override;

  @Override
  public void init(Configuration conf, String keyPrefix) {
    bucketName = conf.get(keyPrefix + S3_BUCKET_NAME);
    String endpoint = conf.get(keyPrefix + S3_ENDPOINT_NAME);
    String key = conf.get(keyPrefix + S3_ACCESS_KEY);
    String secret = conf.get(keyPrefix + S3_ACCESS_SECRET);

    System.setProperty(SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY, key);
    System.setProperty(SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY, secret);
    AWSCredentialsProvider provider = new SystemPropertiesCredentialsProvider();
    client = new AmazonS3Client(provider);
    client.setEndpoint(endpoint);
    override = conf.getBoolean(keyPrefix + "override", true);
    acls = new AccessControlList();
    acls.grantPermission(GroupGrantee.AllUsers, Permission.FullControl);
    acls.grantPermission(GroupGrantee.AllUsers, Permission.Read);
    acls.grantPermission(GroupGrantee.AllUsers, Permission.Write);
  }

  @Override
  public void getFileList(String path, OutputStream out) throws Exception {
    String marker = null;
    do {
      ListObjectsRequest request = new ListObjectsRequest(bucketName, path, null, "/", 1000);
      ObjectListing listing = client.listObjects(request);
      for (S3ObjectSummary object : listing.getObjectSummaries()) {
        String line = object.getKey() + "\n";
        out.write(line.getBytes());
      }
      for (String commonPrefix : listing.getCommonPrefixes()) {
        getFileList(commonPrefix, out);
      }
      marker = listing.getNextMarker();
    } while (marker != null);
  }

  @Override
  public File downloadToFile(String key) throws Exception {
    S3Object object = client.getObject(bucketName, key);
    File file = SourceUtil.getRandomFile();
    OutputStream outputStream = new FileOutputStream(file);
    IOUtils.copy(object.getObjectContent(), outputStream);
    outputStream.flush();
    outputStream.close();
    return file;
  }

  @Override
  public void upload(String key, File file) throws Exception {
    String md5 = SourceUtil.getMD5(file);
    ObjectMetadata meta = client.getObjectMetadata(bucketName, key);
    if (meta == null) {
      client.putObject(bucketName, key, file);
      client.setObjectAcl(bucketName, key, acls);
      meta = client.getObjectMetadata(bucketName, key);
    }
    if (0 == meta.getContentMD5().compareTo(md5)) {
      return;
    }
    if (override) {
      client.putObject(bucketName, key, file);
      client.setObjectAcl(bucketName, key, acls);
      meta = client.getObjectMetadata(bucketName, key);
      if (0 != meta.getContentMD5().compareTo(md5)) {
        throw new IOException("Overide object " + key + " failed to fds");
      }
    }
  }

  @Override
  public boolean exist(String key) throws Exception {
    return client.getObjectMetadata(bucketName, key) != null;
  }
}