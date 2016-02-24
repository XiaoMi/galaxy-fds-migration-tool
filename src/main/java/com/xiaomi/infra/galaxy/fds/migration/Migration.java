package com.xiaomi.infra.galaxy.fds.migration;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.xiaomi.infra.galaxy.fds.source.Source;
import com.xiaomi.infra.galaxy.fds.source.SourceUtil;

public class Migration {
  private static final Log LOG = LogFactory.getLog(Migration.class);

  static enum MigrationCounter {
    TotalObjects, SuccussObjects, FailedObjects
  };

  public static class CopyMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Source from;
    private Source to;
    private Counter totalObjects;
    private Counter failedObjects;
    private Counter succussObjects;
    private boolean skipExistObject;
 
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      from = SourceUtil.getSource(context.getConfiguration(), "migration.source.");
      to = SourceUtil.getSource(context.getConfiguration(), "migration.target.");
      totalObjects = context.getCounter(MigrationCounter.TotalObjects);
      succussObjects = context.getCounter(MigrationCounter.SuccussObjects);
      failedObjects = context.getCounter(MigrationCounter.FailedObjects);
      skipExistObject = context.getConfiguration().getBoolean("migration.skip.exist", true);
    }
 
    private void copy(String objectName) throws Exception {
      File file = null;
      try {
        if (skipExistObject && to.exist(objectName)) {
          return;
        }
        file = from.downloadToFile(objectName);
        to.upload(objectName, file);
      } finally {
        SourceUtil.cleanUp(file);
      }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      String objectName = value.toString().trim();
      LOG.debug("objectName: " + objectName);
      totalObjects.increment(1);
      try {
        copy(objectName);
        succussObjects.increment(1);
      } catch (Exception e) {
        context.write(new Text(objectName), new Text(objectName));
        failedObjects.increment(1);
        LOG.error("Failed to copy object: " + objectName, e);
      }
    }
  }

  public static class IdentityReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
        InterruptedException {
      for (Text val : values) {
        context.write(new Text(), new Text(val.toString()));
      }
    }
  }

  private static void printUsage() {
    System.err.println("Usage: Migration list [-conf job.xml] prefix outputFileName");
    System.err.println("   Or  Migration list/copy [-conf job.xml] <input> <output>");
    System.exit(2);
  }

  public static void main(String[] args) {
    try {
      Configuration conf = new Configuration();
      // Use GenericOptionsParse, supporting -D -conf etc.
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      final FileSystem fs = FileSystem.get(conf);

      if (otherArgs.length != 3) {
        printUsage();
      }

      String cmd = otherArgs[0];
      if (cmd.equals("list")) {
        String prefix = otherArgs[1];
        String outputFile = otherArgs[2];
        LOG.info("Get the file list from source with prefix: " + prefix);
        OutputStream output = fs.create(new Path(outputFile));
        Source from = SourceUtil.getSource(conf, "migration.source.");
        from.getFileList(prefix, output);
        output.close();
      } else if (cmd.equals("copy")) {
        String input = otherArgs[1];
        String output = otherArgs[2];

        System.out.println("Running framework: " + conf.get("mapreduce.framework.name"));
        System.out.println("File system: " + conf.get("fs.default.name"));

        if (conf.getBoolean("migration.cleanup-output", true)) {
          fs.delete(new Path(output), true);
        }

        Job job = new Job(conf, "Galaxy-FDS-Migration-Tool");
        job.setJarByClass(Migration.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(CopyMapper.class);

        job.setNumReduceTasks(conf.getInt("migration.reduce.num", 10));

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(IdentityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
      } else {
        printUsage();
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }
}
