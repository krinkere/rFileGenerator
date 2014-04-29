package com.blogpost.alkrinker.rfilegenerator;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * 
 *
 */
public class Generate {

    private static final String PREFIX = Generate.class.getSimpleName();

    public static final String FILE_TYPE = PREFIX + ".file_type";

    public static void main(String[] args) {
        System.out.println("Executing main method");
        try {
            writeFile();
        } catch (Exception ex) {
            Logger.getLogger(Generate.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static Path writeFile() throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);

        String extension = conf.get(FILE_TYPE);

        if (extension == null || extension.isEmpty()) {
            extension = RFile.EXTENSION;

        }
        String filename = "testFile." + extension;

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
        return file;

    }
}
