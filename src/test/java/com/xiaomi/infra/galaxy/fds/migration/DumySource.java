package com.xiaomi.infra.galaxy.fds.migration;

import java.io.File;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.xiaomi.infra.galaxy.fds.source.Source;

public class DumySource implements Source {
  private static final Log LOG = LogFactory.getLog(DumySource.class);

  @Override
  public void init(Configuration conf, String keyPrefix) {
  }

  @Override
  public File downloadToFile(String key) throws Exception {
    return null;
  }

  @Override
  public void upload(String key, File file) throws Exception {
  }

  @Override
  public boolean exist(String key) throws Exception {
    return false;
  }

  @Override
  public void getFileList(String prefix, OutputStream out) throws Exception {
  }
}

