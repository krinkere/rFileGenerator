package com.blogpost.alkrinker.rfilegenerator;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 *
 * @author Al Krinker
 */
public class GenerateImportRFile {

    // TODO: Get these properties from property file
    private final static String defaultTableName = "sampletable";
    private final static String defaultInputDirectory = "/rfile_import/input_directory";
    private final static String defaultFailedDirectory = "/rfile_import/failed_directory";

    public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        try {
            String tableName;
            String inputDir;
            String failedDir;
            
            if (args.length == 0) {
                tableName = defaultTableName;
                inputDir = defaultInputDirectory;
                failedDir = defaultFailedDirectory;
            }
            else if (args.length == 1) {
                tableName = args[0];
                inputDir = defaultInputDirectory;
                failedDir = defaultFailedDirectory;
            }
            else if (args.length == 2) {
                tableName = args[0];
                inputDir = args[1];
                failedDir = defaultFailedDirectory;
            }
            else {
                tableName = args[0];
                inputDir = args[1];
                failedDir = args[2];
            }
                
            Configuration conf = getConfiguration();
            
            FileSystem fs = FileSystem.get(conf);

            RFileGenerator rFileGenerator = new RFileGenerator();
            rFileGenerator.generateRFile(conf, fs, inputDir, failedDir);
            
            RFileImporter rFileImporter = new RFileImporter();
            rFileImporter.importRFileIntoAccumulo(tableName, inputDir, failedDir);

        } catch (Exception ex) {
            Logger.getLogger(RFileGenerator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static Configuration getConfiguration() {
            
            // Set configuration to our NameNode on Hadoop HDFS system. This way
            // Accumulo will be able to access it and write it to the table. If
            // not specified, it would use our local system to write rFiles.
            Configuration conf = new Configuration();
            // TODO: Get this value from property file
            conf.set("fs.default.name", "hdfs://hadoop.namenode:9000/");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            
            // By default, TableOperations use CachedConfiguration, so when you
            // try to run importdirectory later in the code, it would try to use
            // local system and would complain about files not being found. So 
            // change it to point to Hadoop HDFS 
            CachedConfiguration.setInstance(conf);
            
            return conf;
    }
}
