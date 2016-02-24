package com.xiaomi.infra.galaxy.fds.migration;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.xiaomi.infra.galaxy.fds.migration.Migration;
import com.xiaomi.infra.galaxy.fds.migration.Migration.CopyMapper;
import com.xiaomi.infra.galaxy.fds.migration.Migration.IdentityReducer;

public class MigrationTest {

  MapDriver<LongWritable, Text, Text, Text> mapDriver;
  ReduceDriver<Text, Text, Text, Text> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

  @Before
  public void setUp() {
    CopyMapper mapper = new Migration.CopyMapper();
    IdentityReducer reducer = new Migration.IdentityReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testMapper() throws IOException {
    mapDriver.getConfiguration().set("migration.source.class", DumySource.class.getName());
    mapDriver.getConfiguration().set("migration.target.class", DumySource.class.getName());

    mapDriver.withInput(new LongWritable(1L), new Text("a/b")).withInput(new LongWritable(2L), new Text("c/d"))
        .runTest();

    assertEquals("Expected 2 total obects", 2,
      mapDriver.getCounters().findCounter(Migration.MigrationCounter.TotalObjects).getValue());

    assertEquals("Expected 2 success obects", 2,
      mapDriver.getCounters().findCounter(Migration.MigrationCounter.SuccussObjects).getValue());

    assertEquals("Expected 0 failed obects", 0,
      mapDriver.getCounters().findCounter(Migration.MigrationCounter.FailedObjects).getValue());
  }

  @Test
  public void testReducer() throws IOException {
    List<Text> values = new ArrayList<Text>();
    values.add(new Text("a/b"));
    reduceDriver.withInput(new Text("a/b"), values).withOutput(new Text(), new Text("a/b"))
        .runTest();
  }

  @Test
  public void testMapReduce() throws IOException {
    mapReduceDriver.getConfiguration().set("migration.source.class", DumySource.class.getName());
    mapReduceDriver.getConfiguration().set("migration.target.class", DumySource.class.getName());

    mapReduceDriver.withInput(new LongWritable(1L), new Text("a b"));
    // output key should be in order
    mapReduceDriver.runTest();

    assertEquals("Expected 1 total obects", 1,
      mapReduceDriver.getCounters().findCounter(Migration.MigrationCounter.TotalObjects).getValue());

    assertEquals("Expected 1 success obects", 1,
      mapReduceDriver.getCounters().findCounter(Migration.MigrationCounter.SuccussObjects).getValue());

    assertEquals("Expected 0 failed obects", 0,
      mapReduceDriver.getCounters().findCounter(Migration.MigrationCounter.FailedObjects).getValue());
  }
}