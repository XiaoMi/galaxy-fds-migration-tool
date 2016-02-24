package com.xiaomi.infra.galaxy.fds.source;

import java.io.File;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Source proxy with operation retries
 */

public class SourceWithRetry implements Source {
  private static final Log LOG = LogFactory.getLog(SourceWithRetry.class);
  private Source proxy;
  private int retries;

  public SourceWithRetry(final Source proxy, int retries) {
    this.proxy = proxy;
    this.retries = retries;
  }

  @Override
  public void init(Configuration conf, String keyPrefix) {
    proxy.init(conf, keyPrefix);
  }

  @Override
  public boolean exist(String key) throws Exception {
    long start = System.currentTimeMillis();
    for (int i = 0; i < retries; i++) {
      try {
        boolean exist =  proxy.exist(key);
        long cost = System.currentTimeMillis() - start;
        if (cost > 500) {
          LOG.info("Download file from source cost: " + cost + " ms " + " object: " + key);
        }
        return exist;
      } catch (Exception e) {
        if (i + 1 >= retries) {
          throw e;
        }
        Thread.sleep(3000);
      }
    }
    return false;
  }

  @Override
  public File downloadToFile(String key) throws Exception {
    long start = System.currentTimeMillis();
    for (int i = 0; i < retries; i++) {
      try {
        File file = proxy.downloadToFile(key);
        long cost = System.currentTimeMillis() - start;
        if (cost > 500) {
          LOG.info("Download file from source cost: " + cost + " ms" + " object: " + key);
        }
        return file;
      } catch (Exception e) {
        if (i + 1 >= retries) {
          throw e;
        }
      }
    }
    return null;
  }

  @Override
  public void upload(String key, File file) throws Exception {
    long start = System.currentTimeMillis();
    for (int i = 0; i < retries; i++) {
      try {
        proxy.upload(key, file);
        long cost = System.currentTimeMillis() - start;
        if (cost > 500) {
          LOG.info("Upload file from source cost: " + cost + " ms" + " object: " + key + " size: "
              + file.length());
        }
        return;
      } catch (Exception e) {
        if (i + 1 >= retries) {
          throw e;
        }
      }
    }
  }

  @Override
  public void getFileList(String prefix, OutputStream out) throws Exception {
    proxy.getFileList(prefix, out);
  }
}
