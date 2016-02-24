package com.xiaomi.infra.galaxy.fds.source;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Migration source for HTTP Source
 */

public class HTTPSource implements Source {
  private static final Log LOG = LogFactory.getLog(HTTPSource.class);

  public static final String HTTP_URL_PATTERN = "http.url.pattern";
  public static final String HTTP_DOWNLOADER_LIST = "http.downloader.list";

  private HttpClient client;
  private String urlPattern;
  private String[] ips;
  private Random rand = new Random();

  @Override
  public void init(Configuration conf, String keyPrefix) {
    client = new HttpClient();
    client.getHttpConnectionManager().getParams().setSoTimeout(5 * 1000);
    urlPattern = conf.get(keyPrefix + HTTP_URL_PATTERN);
    ips = conf.get(keyPrefix + HTTP_DOWNLOADER_LIST).split(",");
  }

  
  @Override
  public void getFileList(String prefix, OutputStream out) throws Exception {
    throw new IOException("Not supported in MFS yet");
  }

  @Override
  public File downloadToFile(String key) throws Exception {
    String ip = ips[rand.nextInt(ips.length)];
    String url = String.format(urlPattern, ip, key);
    LOG.debug("Download url:" + url);
    try {
      GetMethod get = new GetMethod(url);
      int code = client.executeMethod(get);
      if (code != HttpStatus.SC_OK) {
        throw new IOException("Failed to download file from url " + url);
      }
      File file = SourceUtil.getRandomFile();
      OutputStream outputStream = new FileOutputStream(file);
      IOUtils.copy(get.getResponseBodyAsStream(), outputStream);
      outputStream.flush();
      outputStream.close();
      return file;
    } catch (Exception e) {
      throw new IOException("Failed to download file from url " + url, e);
    }
  }

  @Override
  public void upload(String key, File file) throws Exception {
    throw new IOException("Not implement yet");
  }

  @Override
  public boolean exist(String key) throws Exception {
    throw new IOException("Not implement yet");
  }

}
