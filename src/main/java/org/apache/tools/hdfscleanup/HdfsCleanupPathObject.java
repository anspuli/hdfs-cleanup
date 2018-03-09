package org.apache.tools.hdfscleanup;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class HdfsCleanupPathObject {
  String pathToCheck = null;
  int lookupLevel = 1;
  int delBatchsize = 10;
  Map<String, Date> myRegMap = new HashMap<String, Date>();
  ArrayList<String> myExcludeList = new ArrayList<String>();
  Map<String, Date> myRetentionMap = new HashMap<String, Date>();
}
