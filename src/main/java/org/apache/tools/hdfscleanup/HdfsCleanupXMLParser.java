package org.apache.tools.hdfscleanup;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


public class HdfsCleanupXMLParser {

  static Logger log = Logger.getLogger(HdfsCleanupXMLParser.class);

  private static String getTagValue(String tag, Element eElement) {
    NodeList nList = eElement.getElementsByTagName(tag).item(0).getChildNodes();
    Node nValue = (Node) nList.item(0);
    if (nValue == null) {
      return null;
    }
    return nValue.getNodeValue();
  }

  public ArrayList<HdfsCleanupPathObject> createHdfsPathObjectFromXML(Document doc, String param) {
    NodeList nList = null;
    NodeList nRegexList = null;
    NodeList nExcludeList = null;
    Node nNode = null;
    Node nRegexNode = null;
    Node nExcludeNode = null;
    int defaultFileRetention = 0;
    int defaultDirRetention = 0;
    int fileRetentionIfEmpty = 0;
    int dirRetentionIfEmpty = 0;
    Date now = new Date();
    long currentTime = now.getTime();
    long hourInMsecs = 1000 * 60 * 60;

    ArrayList<HdfsCleanupPathObject> hdfsPathObjects = new ArrayList<HdfsCleanupPathObject>();

    nList = doc.getElementsByTagName(param);
    if (nList.getLength() < 1) {
      log.info("Cannot find the param " + param + " in the input XML file. Exiting the script");
      System.exit(2);
    } else {
      for (int i = 0; i < nList.getLength(); i++) {
        Map<String, Date> regexMap = new HashMap<String, Date>();
        ArrayList<String> excludeList = new ArrayList<String>();
        nNode = nList.item(i);
        Element element = (Element) nNode;
        HdfsCleanupPathObject hdfsPathObject = new HdfsCleanupPathObject();

        if (getTagValue("path", element) != null) {
          hdfsPathObject.pathToCheck = getTagValue("path", element);
        } else {
          hdfsPathObject.pathToCheck = null;
        }

        if (getTagValue("lookupLevel", element) != null) {
          hdfsPathObject.lookupLevel = Integer.parseInt(getTagValue("lookupLevel", element));
        } else {
          hdfsPathObject.lookupLevel = 1;
        }

        if (getTagValue("delBatchsize", element) != null) {
          hdfsPathObject.delBatchsize = Integer.parseInt(getTagValue("delBatchsize", element));
        } else {
          hdfsPathObject.delBatchsize = 10;
        }

        if (element.getElementsByTagName("defaultFileRetention").item(0).getTextContent().trim() != null) {
          defaultFileRetention =
              Integer.parseInt(element.getElementsByTagName("defaultFileRetention").item(0).getTextContent().trim());
        }

        if (element.getElementsByTagName("defaultDirRetention").item(0).getTextContent().trim() != null) {
          defaultDirRetention =
              Integer.parseInt(element.getElementsByTagName("defaultDirRetention").item(0).getTextContent().trim());
        }

        if (element.getElementsByTagName("fileRetentionIfEmpty").item(0).getTextContent().trim() != null) {
          fileRetentionIfEmpty =
              Integer.parseInt(element.getElementsByTagName("fileRetentionIfEmpty").item(0).getTextContent().trim());
        }
        if (element.getElementsByTagName("fileRetentionIfEmpty").item(0).getTextContent().trim() != null) {
          dirRetentionIfEmpty =
              Integer.parseInt(element.getElementsByTagName("fileRetentionIfEmpty").item(0).getTextContent().trim());
        }

        if ((defaultFileRetention == 0) && (defaultDirRetention == 0) && (fileRetentionIfEmpty == 0)
            && (dirRetentionIfEmpty == 0)) {
          log.error("One or more mandatory filed(s) is/are missing from the XML Configuration file. Exiting the script");
          System.exit(2);
        }

        hdfsPathObject.myRetentionMap.put("defaultFileRetention", new Date(currentTime
            - (defaultFileRetention * hourInMsecs)));
        hdfsPathObject.myRetentionMap.put("defaultDirRetention", new Date(currentTime
            - (defaultDirRetention * hourInMsecs)));
        hdfsPathObject.myRetentionMap.put("fileRetentionIfEmpty", new Date(currentTime
            - (fileRetentionIfEmpty * hourInMsecs)));
        hdfsPathObject.myRetentionMap.put("dirRetentionIfEmpty", new Date(currentTime
            - (dirRetentionIfEmpty * hourInMsecs)));

        /* Finding regex match patterns */
        nRegexList = element.getElementsByTagName("regex");

        for (int j = 0; j < nRegexList.getLength(); j++) {
          String pattern = null;
          int retention;

          nRegexNode = nRegexList.item(j);
          Element elementRegex = (Element) nRegexNode;
          pattern = elementRegex.getElementsByTagName("pattern").item(0).getTextContent().trim();
          retention = Integer.parseInt(elementRegex.getElementsByTagName("retention").item(0).getTextContent().trim());
          regexMap.put(pattern, new Date(currentTime - retention * hourInMsecs));
        }
        hdfsPathObject.myRegMap = regexMap;

        /* Finding list of exclude patterns */
        nExcludeList = element.getElementsByTagName("exclude");

        for (int k = 0; k < nExcludeList.getLength(); k++) {
          nExcludeNode = nExcludeList.item(k);
          Element elementExclude = (Element) nExcludeNode;
          excludeList.add(elementExclude.getElementsByTagName("pattern").item(0).getTextContent().trim());
        }

        hdfsPathObject.myExcludeList = excludeList;

        hdfsPathObjects.add(hdfsPathObject);
      }
    }
    return hdfsPathObjects;
  }

  public boolean validateHdfsPathObject(ArrayList<HdfsCleanupPathObject> hdfsCleanupObjects) {

    for (HdfsCleanupPathObject hdfsObject : hdfsCleanupObjects) {
      if ((hdfsObject.myRetentionMap.get("defaultFileRetention") == null)
          && (hdfsObject.myRetentionMap.get("defaultDirRetention") == null)
          && (hdfsObject.myRetentionMap.get("fileRetentionIfEmpty") == null)
          && (hdfsObject.myRetentionMap.get("dirRetentionIfEmpty") == null) && (hdfsObject.lookupLevel < 1)) {
        return false;
      }
    }
    return true;
  }
}
