package org.apache.tools.hdfscleanup;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;


public class HdfsCleanupUtils {

  Logger log = Logger.getLogger(HdfsCleanupUtils.class);

  public HdfsCleanupUtils() {

  }

  public FileSystem hdfsSimpleConnect(Configuration conf) throws IOException {
    FileSystem hdfs = FileSystem.get(conf);
    return hdfs;
  }

  public FileSystem hdfsKerberosConnect(Configuration conf, String user, String keytab) throws IOException {

    //  authentication using hdfs username for production
    //    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(user, keytab);
    FileSystem hdfs = FileSystem.get(conf);
    return hdfs;
  }

  /* If the path is older than 7 days => delete it
   * else if the path is empty and older than one day => delete it
   * else if the path is a file and is older than 3 days => delete it
   * else if the path is a directory and older than 1 day.. then
   *          if it matches vd push dirs => delete it
   *          if it matches crunch directory => delete it
   * else.. don't delete the path
   */

  public boolean checkPathForDeletion(FileStatus fStatus, FileSystem fs, HdfsCleanupPathObject hdfsPathObject)
      throws IOException {

    long spaceUsed;
    Path pathName = fStatus.getPath();
    long modificationTime = fStatus.getModificationTime();
    try {
      ContentSummary contentSummary = fs.getContentSummary(fStatus.getPath());
      spaceUsed = contentSummary.getSpaceConsumed();
    } catch (FileNotFoundException e) {
      log.warn(pathName.toString() + " does not exist now");
      return false;
    }
    //    String absFileName = new File(pathName.toString()).getName();
    log.info("[" + hdfsPathObject.pathToCheck + "] * Checking the path: " + fStatus.getPath().toString());
    log.info("[" + hdfsPathObject.pathToCheck + "] The size of " + pathName.toString() + " is: " + spaceUsed / 1024
        + "KB");
    log.info("[" + hdfsPathObject.pathToCheck + "] The owner of " + pathName.toString() + " is: " + fStatus.getOwner());

    log.info("[" + hdfsPathObject.pathToCheck + "] Checking if the file/directory name " + pathName.getName()
        + " matches the exclude list");
    for (int k = 0; k < hdfsPathObject.myExcludeList.size(); k++) {
      if (pathName.getName().matches(hdfsPathObject.myExcludeList.get(k))) {
        log.info("[" + hdfsPathObject.pathToCheck + "] " + pathName.toString() + " matches exclude pattern: "
            + hdfsPathObject.myExcludeList.get(k) + ". Skipping it");
        return false;
      }
    }

    if (fStatus.isDirectory()) {
      log.info("[" + hdfsPathObject.pathToCheck + "] " + pathName.toString() + " is a Directory");

      log.info("[" + hdfsPathObject.pathToCheck + "] The File modification time is " + new Date(modificationTime));
      log.info("[" + hdfsPathObject.pathToCheck + "] defaultDirRetentionTime is "
          + hdfsPathObject.myRetentionMap.get("defaultDirRetention"));
      if (modificationTime < hdfsPathObject.myRetentionMap.get("defaultDirRetention").getTime()) {
        log.info("[" + hdfsPathObject.pathToCheck + "] " + pathName.toString() + " is older than "
            + hdfsPathObject.myRetentionMap.get("defaultDirRetention") + ". Marking it for deletion");
        return true;
      } else if (spaceUsed == 0
          && modificationTime < hdfsPathObject.myRetentionMap.get("dirRetentionIfEmpty").getTime()) {
        log.info("[" + hdfsPathObject.pathToCheck + "] " + pathName.toString() + " is empty and is older than "
            + hdfsPathObject.myRetentionMap.get("dirRetentionIfEmpty") + ". Marking it for deletion");
        return true;
      } else if (!hdfsPathObject.myRegMap.isEmpty()) {
        log.info("[" + hdfsPathObject.pathToCheck + "] Checking for regex");

        for (Entry<String, Date> entry : hdfsPathObject.myRegMap.entrySet()) {
          if (pathName.getName().matches(entry.getKey()) && modificationTime < entry.getValue().getTime()) {
            log.info("[" + hdfsPathObject.pathToCheck + "] " + pathName.toString() + " matches the regex "
                + entry.getKey() + " and is older than " + entry.getValue() + ". Marking it for deletion");
            return true;
          }
        }
        log.info("[" + hdfsPathObject.pathToCheck + "] " + pathName.toString()
            + " does not matches any deletion criteria");
      }
    } else if (fStatus.isFile()) {
      log.info("[" + hdfsPathObject.pathToCheck + "] " + pathName.toString() + " is a File");
      if (modificationTime < hdfsPathObject.myRetentionMap.get("defaultFileRetention").getTime()) {
        log.info("[" + hdfsPathObject.pathToCheck + "] " + pathName.toString() + " is older than "
            + hdfsPathObject.myRetentionMap.get("defaultFileRetention") + " Marking it for deletion");
        return true;
      } else if (spaceUsed == 0
          && modificationTime < hdfsPathObject.myRetentionMap.get("fileRetentionIfEmpty").getTime()) {
        log.info("[" + hdfsPathObject.pathToCheck + "] " + pathName.toString() + " is empty and is older than "
            + hdfsPathObject.myRetentionMap.get("fileRetentionIfEmpty") + ". Marking it for deletion");
        return true;
      } else {
        log.info("[" + hdfsPathObject.pathToCheck + "] " + pathName.toString()
            + " does not match any deletion criteria");
      }
    } else {
      log.info("[" + hdfsPathObject.pathToCheck + "] Cannot find file type..skipping " + pathName.toString());
      return false;
    }
    return false;
  }

  public boolean deletePath(Path pathtoDelete, FileSystem hdfs) throws IOException {
    try {
      hdfs.delete(pathtoDelete, true);
      log.info(pathtoDelete + " deleted successfully");
      return true;
    } catch (RemoteException e) {
      log.error("Unable to delete the file " + pathtoDelete);
      log.error(e);
      return false;
    }
  }

  public void closeConnection(FileSystem hdfs) throws IOException {
    hdfs.close();
  }

}
