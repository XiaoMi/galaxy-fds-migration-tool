package com.xiaomi.infra.galaxy.fds.source;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.xiaomi.infra.galaxy.fds.client.FDSClientConfiguration;
import com.xiaomi.infra.galaxy.fds.client.GalaxyFDSClient;
import com.xiaomi.infra.galaxy.fds.client.credential.BasicFDSCredential;
import com.xiaomi.infra.galaxy.fds.client.credential.GalaxyFDSCredential;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObject;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectListing;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectSummary;
import com.xiaomi.infra.galaxy.fds.model.AccessControlList;
import com.xiaomi.infra.galaxy.fds.model.AccessControlList.Grant;
import com.xiaomi.infra.galaxy.fds.model.AccessControlList.GrantType;
import com.xiaomi.infra.galaxy.fds.model.AccessControlList.Permission;
import com.xiaomi.infra.galaxy.fds.model.AccessControlList.UserGroups;
import com.xiaomi.infra.galaxy.fds.model.FDSObjectMetadata;

/**
 * Migration source for XIAOMI Galaxy FDS
 */
public class FDSSource implements Source {
  private static final Log LOG = LogFactory.getLog(FDSSource.class);
  public static final String FDS_REGION_NAME = "fds.region.name";
  public static final String FDS_BUCKET_NAME = "fds.bucket.name";
  public static final String FDS_ACCESS_KEY = "fds.access.key";
  public static final String FDS_ACCESS_SECRET = "fds.access.secret";

  private String bucketName;
  private GalaxyFDSClient client;
  private AccessControlList acl;
  private boolean override;

  @Override
  public void init(Configuration conf, String keyPrefix) {
    bucketName = conf.get(keyPrefix + FDS_BUCKET_NAME);
    GalaxyFDSCredential credential =
        new BasicFDSCredential(conf.get(keyPrefix + FDS_ACCESS_KEY), conf.get(keyPrefix
            + FDS_ACCESS_SECRET));
    FDSClientConfiguration fdsConfig = new FDSClientConfiguration();
    fdsConfig.enableHttps(false);
    fdsConfig.enableCdnForUpload(false);
    fdsConfig.enableCdnForDownload(false);
    fdsConfig.setRegionName(conf.get(keyPrefix + FDS_REGION_NAME));
    fdsConfig.setEnableMd5Calculate(true);
    client = new GalaxyFDSClient(credential, fdsConfig);

    acl = new AccessControlList();
    acl.addGrant(new Grant(UserGroups.ALL_USERS.name(), Permission.READ, GrantType.GROUP));

    override = conf.getBoolean(keyPrefix + "override", true);
  }

  @Override
  public void getFileList(String path, OutputStream out) throws Exception {
    FDSObjectListing listing = client.listObjects(bucketName, path, "");
    while (true) {
      LOG.info("Listed objects numbers: " + listing.getObjectSummaries().size());
      for (FDSObjectSummary object : listing.getObjectSummaries()) {
        String line = object.getObjectName() + "\n";
        out.write(line.getBytes());
      }
      if (!listing.isTruncated()) {
        break;
      }
      listing.setMaxKeys(10000);
      long start = System.currentTimeMillis();
      listing = client.listNextBatchOfObjects(listing);
      if (listing == null)  {
        break;
      }
      long cost = System.currentTimeMillis() - start;
      if (cost > 400) {
        LOG.info("Slow listNextBatchOfObjects cost " + cost + " ms");
      }
    }
  }

  @Override
  public File downloadToFile(String key) throws Exception {
    FDSObject object = client.getObject(bucketName, key);
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
    if (!exist(key)) {
      client.putObject(bucketName, key, file);
      client.setObjectAcl(bucketName, key, acl);
    }
    FDSObjectMetadata meta = client.getObjectMetadata(bucketName, key);
    if (0 == meta.getContentMD5().compareTo(md5)) {
      return;
    }
    if (override) {
      client.putObject(bucketName, key, file);
      client.setObjectAcl(bucketName, key, acl);
      meta = client.getObjectMetadata(bucketName, key);
      if (0 != meta.getContentMD5().compareTo(md5)) {
        throw new IOException("Overide object " + key + " failed to fds");
      }
    }
  }

  @Override
  public boolean exist(String key) throws Exception {
    return client.doesObjectExist(bucketName, key);
  }
}
