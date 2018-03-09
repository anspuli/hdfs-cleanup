package org.apache.tools.hdfscleanup;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HdfsCleanupFindPaths {

  public ArrayList<Path> findPathsForDeletion(Path path, FileSystem fs, HdfsCleanupPathObject hdfsPathObject,
      int lookupLevel) throws IOException {
    HdfsCleanupUtils hcleanup = new HdfsCleanupUtils();
    ArrayList<Path> pathsToDelete = new ArrayList<Path>();
    FileStatus[] fileStatus = fs.listStatus(path);
    int lookup = --lookupLevel;
    for (FileStatus filestat : fileStatus) {
      if (lookup == 0) {
        if (hcleanup.checkPathForDeletion(filestat, fs, hdfsPathObject)) {
          pathsToDelete.add(filestat.getPath());
        }
      } else if (filestat.isDir()) {
        pathsToDelete.addAll(findPathsForDeletion(filestat.getPath(), fs, hdfsPathObject, lookup));
      }
    }
    return pathsToDelete;
  }
}
