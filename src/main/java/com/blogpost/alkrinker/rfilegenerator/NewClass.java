package com.blogpost.alkrinker.rfilegenerator;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class NewClass {

    private static final String PREFIX = RFileGenerator.class.getSimpleName();

    public static final String FILE_TYPE = PREFIX + ".rf";

    private static final String inputDirectory = "/rfile_import/input_directory";
    private static final String failedDirectory = "/rfile_import/failed_directory";

    public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        try {
            // Set configuration to our NameNode on Hadoop HDFS system. This way
            // Accumulo will be able to access it and write it to the table. If
            // not specified, it would use our local system to write files.
            Configuration conf = new Configuration();
            conf.set("fs.default.name", "hdfs://hadoop.namenode:9000/");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            
            // By default, TableOperations use CachedConfiguration, so when you
            // try to run importdirectory later in the code, it would try to use
            // local system and would complain about files not being found. So
            // change it to point to Hadoop HDFS
            CachedConfiguration.setInstance(conf);
            
            FileSystem fs = FileSystem.get(conf);

            // Now that we have FileSystem access, create directory to store rFile,
            // and additional directory to store failed rFiles on import to Accumulo
            createDirectories(conf, fs);
            
            createRFile(conf, fs);
            
            importRFileIntoAccumulo();

        } catch (Exception ex) {
            Logger.getLogger(RFileGenerator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void createDirectories(Configuration conf, FileSystem fs) throws Exception {

        Path input = new Path(inputDirectory);
        Path failput = new Path(failedDirectory);
        fs.mkdirs(input);
        fs.mkdirs(failput);
    
    }

    public static void createRFile(Configuration conf, FileSystem fs) throws Exception {

        String extension = conf.get(FILE_TYPE);

        if (extension == null || extension.isEmpty()) {
            extension = RFile.EXTENSION;
        }

        String filename = inputDirectory + "testFile." + extension;

        Path file = new Path(filename);

        if (fs.exists(file)) {
            file.getFileSystem(conf).delete(file, false);
        }

        FileSKVWriter out = RFileOperations.getInstance().openWriter(filename, fs, conf,
                AccumuloConfiguration.getDefaultConfiguration());
        out.startDefaultLocalityGroup();

        long timestamp = (new Date()).getTime();

        Key key = new Key(new Text("row_1"), new Text("cf"), new Text("cq"),
                new ColumnVisibility(), timestamp);
        Value value = new Value("".getBytes());

        out.append(key, value);
        out.close();
    }
    
    public static void importRFileIntoAccumulo() throws Exception
    {
        // Provide instanceName
        String instanceName = "default";
        // Provide list of zookeeper server here. 
        String zooServers = "zookeeper1:2181,zookeeper2:2181,zookeeper3:2181"; 
        // Provide username
        String userName = "root"; 
        // Provide password
        String password = "secret"; 
        
        // Connect
        Instance inst = new ZooKeeperInstance(instanceName, zooServers);
        Connector conn = inst.getConnector(userName, password);
        TableOperations ops = conn.tableOperations();
        
        if (ops.exists("sampletable")) {
            ops.delete("sampletable");
        }
        ops.create("sampletable");
        
        ops.importDirectory("sampletable", inputDirectory, failedDirectory, false);
        ops.compact("sampletable", null, null, true, false);
    }

}