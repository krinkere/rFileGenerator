package com.blogpost.alkrinker.rfilegenerator;

import java.util.Date;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 *
 * @author Al Krinker
 */
public class RFileGenerator {

    private static final String PREFIX = RFileGenerator.class.getSimpleName();

    public static final String FILE_TYPE = PREFIX + ".rf";

    public void createDirectories(Configuration conf, FileSystem fs, String inputDir, String failedDir) throws Exception {

        //System.out.println(fs.getHomeDirectory());
        Path input = new Path(inputDir);
        Path failput = new Path(failedDir);
        fs.mkdirs(input);
        fs.mkdirs(failput);

        // Tweek permissions if needed
        //fs.setPermission(input, FsPermission.createImmutable((short) 0777));
        //fs.setOwner(input, "accumulo", "supergroup");
        //fs.setOwner(output, "accumulo", "supergroup");
    }

    public void generateRFile(Configuration conf, FileSystem fs, String inputDir, String failedDir) throws Exception {

        createDirectories(conf, fs, inputDir, failedDir);
        
        String extension = conf.get(FILE_TYPE);

        if (extension == null || extension.isEmpty()) {
            extension = RFile.EXTENSION;
        }

        String filename = inputDir + "testFile." + extension;

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
    
    
}
