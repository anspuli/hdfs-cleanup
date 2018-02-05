package org.apache.tools.hdfscleanup; 

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.tools.hdfscleanup.HdfsCleanupUtils;


public class TestHdfsCleanup {
  private Configuration conf;
  private Path tmpPath;
  private FileSystem fs;
  private Path dir1, dir2, dir3, dir4, dir5, dir6;
  private Path file1, file2, file3, file4, file5,file6;
  ArrayList<Path> filesToCreate = new ArrayList<Path>();
  ArrayList<Path> dirsToCreate = new ArrayList<Path>();
  ArrayList<String> testMyExcludeList = new ArrayList<String>();
  private FileStatus dir1Status, dir2Status, dir3Status, dir4Status, dir5Status, dir6Status;
  private FileStatus file1Status, file3Status, file4Status;
  private HdfsCleanupUtils tcpTest;
  private long dayInMillis = 1000 * 60 * 60 * 24;
  private Date now = new Date();
  private long currentTime = now.getTime();
  private Date defaultFileRetentionTime = new Date(currentTime - 3 * dayInMillis);
  private Date defaultDirRetentionTime = new Date(currentTime - 7 * dayInMillis);
  private Date dirRetentionIfEmptyTime = new Date(currentTime - 1 * dayInMillis);
  private Date fileRetentionIfEmptyTime = new Date(currentTime - 1 * dayInMillis);
  private Map<String, Date> testMyRegDateMap = new HashMap<String, Date>();
  private HdfsCleanupPathObject testHdfsPathObject = new HdfsCleanupPathObject();

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    tmpPath = new Path("target/tmp/");
    dir1 = new Path(tmpPath + "/test_dir1");
    dir2 = new Path(tmpPath + "/test_dir2");
    dir3 = new Path(tmpPath + "/vold-build-and-push-12123213");
    dir4 = new Path(tmpPath + "/crunch-23423534");
    dir5 = new Path(tmpPath + "/test_dir3");
    dir6 = new Path(tmpPath + "/hive-test");

    dirsToCreate.add(dir1);
    dirsToCreate.add(dir2);
    dirsToCreate.add(dir3);
    dirsToCreate.add(dir4);
    dirsToCreate.add(dir5);
    dirsToCreate.add(dir6);

    file1 = new Path(tmpPath + "/test_file1");
    file2 = new Path(tmpPath + "/test_file2");
    file3 = new Path(tmpPath + "/test_file3");
    file4 = new Path(tmpPath + "/test_file4");
    file5 = new Path(dir3 + "/test_file5");
    file6 = new Path(dir4 + "/test_file5");

    filesToCreate.add(file1);
    filesToCreate.add(file2);
    filesToCreate.add(file3);
    filesToCreate.add(file4);
    filesToCreate.add(file5);

    fs = FileSystem.getLocal(conf);
    fs.delete(tmpPath, true);

    for (Path p : filesToCreate) {
      FSDataOutputStream ostream = fs.create(p);
      ostream.write("Hello World".getBytes());
      ostream.flush();
      ostream.sync();
      ostream.close();
    }

    for (Path p : dirsToCreate) {
      fs.mkdirs(p);
    }

    /* Preparing input data */

    /*1. Create a Directory ( dir1 ) with 7 day old timestamp.*/
    fs.setTimes(dir1, defaultDirRetentionTime.getTime(), defaultDirRetentionTime.getTime());
    /*2. Create a file ( file1 ) with 7 days old timestamp */
    fs.setTimes(file1, defaultFileRetentionTime.getTime(), defaultFileRetentionTime.getTime());
    /*3. Create a empty directory with 1 days old timestamp (dir2). */
    fs.setTimes(dir2, dirRetentionIfEmptyTime.getTime(), dirRetentionIfEmptyTime.getTime());
    /*4.Create a empty file with 1 days old timestamp (file1) */
    fs.setTimes(file2, fileRetentionIfEmptyTime.getTime(), fileRetentionIfEmptyTime.getTime());
    /*5. Create a file with 3 days old timestamp */
    fs.setTimes(file3, defaultFileRetentionTime.getTime(), defaultFileRetentionTime.getTime());
    /*6 Create a directory with name "vold-build-and-push-12123213" and one day old */
    fs.setTimes(dir3, dirRetentionIfEmptyTime.getTime(), dirRetentionIfEmptyTime.getTime());
    /*7 Create a directory with name "crunch-23423534" and one day old */
    fs.setTimes(dir4, dirRetentionIfEmptyTime.getTime(), dirRetentionIfEmptyTime.getTime());
    /* Create a directory with current timestamp - Already created dir5 for this */
    /* Create a directory with current timestamp - Already created file4 for this */

    testHdfsPathObject.myRetentionMap.put("defaultDirRetention", defaultDirRetentionTime);
    testHdfsPathObject.myRetentionMap.put("defaultFileRetention", defaultFileRetentionTime);
    testHdfsPathObject.myRetentionMap.put("dirRetentionIfEmpty", dirRetentionIfEmptyTime);
    testHdfsPathObject.myRetentionMap.put("fileRetentionIfEmpty", defaultDirRetentionTime);

    testMyRegDateMap.put("^vold-build-and-push-[0-9].*", fileRetentionIfEmptyTime);
    testMyRegDateMap.put("^crunch-[0-9].*", fileRetentionIfEmptyTime);
    testMyExcludeList.add("hive-*");

    testHdfsPathObject.myRegMap = testMyRegDateMap;
    testHdfsPathObject.myExcludeList = testMyExcludeList;

    tcpTest = new HdfsCleanupUtils();
  }

  @Test
  public void TestDirSevenDaysOld() throws IOException {
    dir1Status = fs.getFileStatus(dir1);
    Assert.assertEquals(true, tcpTest.checkPathForDeletion(dir1Status, fs, testHdfsPathObject));
  }

  @Test
  public void TestFileSevenDaysOld() throws IOException {
    file1Status = fs.getFileStatus(file1);
    Assert.assertEquals(true, tcpTest.checkPathForDeletion(file1Status, fs,testHdfsPathObject));
  }

  @Test
  public void TestDirEmptyAndOneDayOld() throws IOException {
    dir2Status = fs.getFileStatus(dir2);
    Assert.assertEquals(true, tcpTest.checkPathForDeletion(dir2Status, fs, testHdfsPathObject));
  }

  @Test
  public void TestFileThreeDaysOld() throws IOException {
    file3Status = fs.getFileStatus(file3);
    Assert.assertEquals(true, tcpTest.checkPathForDeletion(file3Status, fs, testHdfsPathObject));
  }

  @Test
  public void TestDirVoldermantAndOneDayOld() throws IOException {
    dir3Status = fs.getFileStatus(dir3);
    Assert.assertEquals(true, tcpTest.checkPathForDeletion(dir3Status, fs, testHdfsPathObject));
  }

  @Test
  public void TestDirCrunchAndOneDayOld() throws IOException {
    dir4Status = fs.getFileStatus(dir4);
    Assert.assertEquals(true, tcpTest.checkPathForDeletion(dir4Status, fs, testHdfsPathObject));
  }

  @Test
  public void TestDirCurrentTimeStamp() throws IOException {
    dir5Status = fs.getFileStatus(dir5);
    Assert.assertEquals(false, tcpTest.checkPathForDeletion(dir5Status, fs, testHdfsPathObject));
  }

  @Test
  public void TestDirExcludePattern() throws IOException {
    dir6Status = fs.getFileStatus(dir6);
    Assert.assertEquals(false, tcpTest.checkPathForDeletion(dir6Status, fs, testHdfsPathObject));
  }

  @Test
  public void TestFileCurrentTimeStamp() throws IOException {
    file4Status = fs.getFileStatus(file4);
    Assert.assertEquals(false, tcpTest.checkPathForDeletion(file4Status, fs, testHdfsPathObject));
  }

}
