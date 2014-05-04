package com.blogpost.alkrinker.rfilegenerator;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

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

public class Generate {

    private static final String PREFIX = Generate.class.getSimpleName();

    public static final String FILE_TYPE = PREFIX + ".file_type";

    private static final String inputDirectory = "/rfile_import/input_directory";
    private static final String failedDirectory = "/rfile_import/failed_directory";

    public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        try {
            // Set configuration to our NameNode on Hadoop HDFS system. This way
            // Accumulo will be able to access it and write it to the table. If
            // not specified, it would use our local system to write such files.
            Configuration conf = new Configuration();
            conf.set("fs.default.name", "hdfs://hadoop.namenode:9000/");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            
            // By default, TableOperations use CachedConfiguration, so when you
            // try to run importdirectory later in the code, it would try to use
            // local system and would complain about files not being found. So 
            // change it to point to Hadoop HDFS 
            CachedConfiguration.setInstance(conf);
            
            FileSystem fs = FileSystem.get(conf);

            createDirectories(conf, fs);
            createRFile(conf, fs);
            importRFileIntoAccumulo();

        } catch (Exception ex) {
            Logger.getLogger(Generate.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void createDirectories(Configuration conf, FileSystem fs) throws Exception {

        //System.out.println(fs.getHomeDirectory());
        Path input = new Path(inputDirectory);
        Path failput = new Path(failedDirectory);
        fs.mkdirs(input);
        fs.mkdirs(failput);

        // Tweek permissions if needed
        //fs.setPermission(input, FsPermission.createImmutable((short) 0777));
        //fs.setOwner(input, "accumulo", "supergroup");
        //fs.setOwner(output, "accumulo", "supergroup");
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
        // Constants
        String instanceName = "default";
        String zooServers = "zookeeper1:2181,zookeeper2:2181,zookeeper3:2181"; // Provide list of zookeeper server here. In our case, we had just one so localhost:2181 should do 
        String userName = "root"; // Provide username
        String password = "secret"; // Provide password
        
        // Connect
        Instance inst = new ZooKeeperInstance(instanceName, zooServers);
        Connector conn = inst.getConnector(userName, password);
        TableOperations ops = conn.tableOperations();
        
        ops.delete("mynewtesttable");
        ops.create("mynewtesttable");
        
        //System.out.println(inputDirectory);
        //System.out.println(failedDirectory);
        ops.importDirectory("mynewtesttable", inputDirectory, failedDirectory, false);
        ops.compact("mynewtesttable", null, null, true, false);
    }

}
