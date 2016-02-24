package com.xiaomi.infra.galaxy.fds.source;

import java.io.File;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;

public interface Source {

  public void init(final Configuration conf, String keyPrefix);

  public void getFileList(String prefix, OutputStream out) throws Exception;

  public boolean exist(String key) throws Exception;

  public File downloadToFile(String key) throws Exception;

  public void upload(String key, File file) throws Exception;
}
