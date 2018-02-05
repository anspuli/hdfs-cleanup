package org.apache.tools.hdfscleanup;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.Logger;


public class HdfsCleanup {
  private HdfsCleanup() {
  }

  public static void main(String[] args) throws IOException, RemoteException, ConnectException, InterruptedException,
      ParserConfigurationException, ParseException {
    Logger log = Logger.getLogger(HdfsCleanup.class);

    String configFile = null;
    String user = null;
    String keytab = null;
    boolean dryrun = false;
    boolean dev = false;
    int lookupLevel;
    ArrayList<HdfsCleanupPathObject> hdfsObjects = new ArrayList<HdfsCleanupPathObject>();
    Date now = new Date();
    ArrayList<Path> pathsToDelete = new ArrayList<Path>();
    String[] dirExcludeList = { "/", "/projects", "/data", "/jobs" };

    Options options = new Options();

    options.addOption("d", "dryrun", false, "Dry Run Mode");
    options.addOption("dev", false, "Development Mode");
    options.addOption("h", "help", false, "Print the help message");
    options.addOption("k", "keytab", true, "User keytab location");
    options.addOption("u", "user", true, "Kerberos user principle (eg: hdfs@GRID.LINKEDIIN.COM )");
    options.addOption(OptionBuilder.withArgName("configFile").hasArg().withDescription("The XML configuration file")
        .create("config"));

    CommandLineParser parser = new GnuParser();
    CommandLine cli = parser.parse(options, args);
    HelpFormatter formatter = new HelpFormatter();

    if (cli.hasOption("help")) {
      formatter.printHelp("hadoop jar grid-ops-tools.*.jar com.linkedin.gridops.hdfscleanup.HdfsCleanup ", options);
      System.exit(2);
    }

    if (cli.hasOption("config")) {
      configFile = cli.getOptionValue("config");
    } else {
      log.error("XML config file is not specified. Use '--help' to see the cli options");
      System.exit(2);
    }

    if (cli.hasOption("dryrun")) {
      log.info("Running in DRY-RUN Mode");
      dryrun = true;
    }

    if (cli.hasOption("dev")) {
      dev = true;
      log.info("Running in development Mode ( hdfs://localhost:9000 )");
    } else {
      if (cli.hasOption("keytab") && cli.hasOption("user")) {
        user = cli.getOptionValue("user");
        keytab = cli.getOptionValue("keytab");
        log.info("User principle is " + user + " and keytab is " + keytab);
      } else {
        log.error("Both user and keytab has to be specified. Use '--help' to see the cli options");
        System.exit(2);
      }
    }

    log.info("Current time is " + now);
    log.info("XML Config file is " + configFile);
    File file = new File(configFile);
    if (!file.exists()) {
      log.error("Config File " + configFile + " does not exist.. Exiting the script");
      System.exit(2);
    }

    //parsing XML configuration file
    log.info("Reading Configurations from XML Config file " + configFile);
    DocumentBuilderFactory dbfactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dbbuilder = dbfactory.newDocumentBuilder();
    org.w3c.dom.Document doc = null;

    try {
      doc = dbbuilder.parse(configFile);
    } catch (org.xml.sax.SAXException e) {
      log.error("Exceptions while opening " + configFile);
      System.exit(2);
    }

    Configuration conf = new Configuration();
    HdfsCleanupXMLParser xmlParser = new HdfsCleanupXMLParser();

    hdfsObjects = xmlParser.createHdfsPathObjectFromXML(doc, "dirPath");

    if (!xmlParser.validateHdfsPathObject(hdfsObjects)) {
      log.error("Mandatory fields are missing from the XML configurations. Exiting the script");
      System.exit(2);
    }

    if (!hdfsObjects.isEmpty()) {
      log.info("There are " + hdfsObjects.size() + " HDFS paths configured for cleanup");
      for (int i = 0; i < hdfsObjects.size(); i++) {
        System.out.println();
        log.info("Hdfs paths to cleanup: " + hdfsObjects.get(i).pathToCheck);
        log.info("[" + hdfsObjects.get(i).pathToCheck + "] Hdfs directory lookupLevel: "
            + hdfsObjects.get(i).lookupLevel);
        log.info("[" + hdfsObjects.get(i).pathToCheck + "] Hdfs defaultFileRetention: "
            + hdfsObjects.get(i).myRetentionMap.get("defaultFileRetention"));
        log.info("[" + hdfsObjects.get(i).pathToCheck + "] Hdfs defaultDirRetention: "
            + hdfsObjects.get(i).myRetentionMap.get("defaultDirRetention"));
        log.info("[" + hdfsObjects.get(i).pathToCheck + "] Hdfs fileRetentionIfEmpty: "
            + hdfsObjects.get(i).myRetentionMap.get("fileRetentionIfEmpty"));
        log.info("[" + hdfsObjects.get(i).pathToCheck + "] Hdfs dirRetentionIfEmpty: "
            + hdfsObjects.get(i).myRetentionMap.get("dirRetentionIfEmpty"));
        log.info("[" + hdfsObjects.get(i).pathToCheck + "] File/Dir regex patterns and retentions: "
            + hdfsObjects.get(i).myRegMap);
        log.info("[" + hdfsObjects.get(i).pathToCheck + "] File/Dir exclude patterns are: "
            + hdfsObjects.get(i).myExcludeList);

        if (Arrays.asList(dirExcludeList).contains(hdfsObjects.get(i).pathToCheck.toString())) {
          log.error(hdfsObjects.get(i).pathToCheck.toString() + " is in the list of excludeded directories - "
              + Arrays.toString(dirExcludeList) + "Exiting the script");
          System.exit(2);
        }

        lookupLevel = hdfsObjects.get(i).lookupLevel;
        Path path = new Path(hdfsObjects.get(i).pathToCheck);

        HdfsCleanupUtils hcp = new HdfsCleanupUtils();
        HdfsCleanupFindPaths hfindpaths = new HdfsCleanupFindPaths();
        FileSystem hdfs = hcp.hdfsConnect(dev, conf, user, keytab);

        try {
          if (!hdfs.exists(path)) {
            log.error(path.toString() + "path " + path.toString() + "does not exist in HDFS. Exiting the script");
            System.exit(2);
          }
        } catch (ConnectException e) {
          log.error("Unable to connect to the HDFS cluster. Exiting the script");
          log.error(e);
          System.exit(2);
        }

        ContentSummary cSummary = hdfs.getContentSummary(path);
        log.info("[" + hdfsObjects.get(i).pathToCheck + "] Found a total of " + cSummary.getDirectoryCount()
            + " Directories and " + cSummary.getFileCount() + " Files");
        log.info("[" + hdfsObjects.get(i).pathToCheck + "] Total " + path.toString() + " space usage is: "
            + (float) cSummary.getSpaceConsumed() / 1024 / 1024 / 1024 + "GB");
        log.info("[" + hdfsObjects.get(i).pathToCheck + "] Checking directories/files for cleanup");

        pathsToDelete = hfindpaths.findPathsForDeletion(path, hdfs, hdfsObjects.get(i), lookupLevel);

        if (pathsToDelete.size() > 0) {
          int numDel = 0;
          log.info("[" + hdfsObjects.get(i).pathToCheck + "] There are " + pathsToDelete.size()
              + " files/directories to delete");
          for (int k = 0; k < pathsToDelete.size(); k++) {
            if (dryrun) {
              log.info("[" + hdfsObjects.get(i).pathToCheck + "] DRY-RUN " + pathsToDelete.get(k));
            } else {
              numDel = numDel + 1;
              log.info("[" + hdfsObjects.get(i).pathToCheck + "] Deleting the file " + pathsToDelete.get(k));
              hcp.deletePath(pathsToDelete.get(k), hdfs);
              if (numDel % 10 == 0) {
                log.info("[" + hdfsObjects.get(i).pathToCheck + "] Deleted " + numDel
                    + " files. Sleeping for 60seconds");
                Thread.sleep(60000);
              }
            }
          }
        } else {
          log.info("[" + hdfsObjects.get(i).pathToCheck + "] Zero files/directories to Delete");
        }
        hcp.closeConnection(hdfs);
      }
    } else {
      log.error("Cannot find any Paths to cleanup in XML configuration file " + configFile);
      System.exit(2);
    }
  }
}
