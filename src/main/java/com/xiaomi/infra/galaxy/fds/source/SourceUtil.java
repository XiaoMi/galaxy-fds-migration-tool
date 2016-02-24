package com.xiaomi.infra.galaxy.fds.source;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;

public class SourceUtil {
  public static String getMD5(File file) throws IOException {
    FileInputStream fis = new FileInputStream(file);
    String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
    fis.close();
    return md5;
  }

  public static Source getSource(Configuration conf, String keyPrefix) throws IOException {
    String name = conf.get(keyPrefix + "class");
    try {
      Class<? extends Source> className = (Class<? extends Source>) Class.forName(name);
      Constructor<? extends Source> method = className.getConstructor();
      method.setAccessible(true);
      Source source = method.newInstance();
      source.init(conf, keyPrefix);
      return new SourceWithRetry(source, conf.getInt("migration.operation.retries", 3));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static void cleanUp(File file) {
    if (file != null) {
      file.delete();
    }
  }

  public static File getRandomFile() {
    return new File("./" + RandomStringUtils.randomAlphanumeric(20));
  }
}
