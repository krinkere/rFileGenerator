package com.blogpost.alkrinker.rfilegenerator;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;

/**
 *
 * @author Al Krinker
 */
public class RFileImporter {
    
    private Connector connectorBuilder() throws AccumuloException, AccumuloSecurityException
    {
        // TODO: Make this read from a property file.
        String instanceName = "default";
        // Provide list of zookeeper server here. In our case, we had just one so localhost:2181 should do 
        String zooServers = "zookeeper1:2181,zookeeper2:2181,zookeeper3:2181"; 
        String userName = "root"; 
        // TODO: Encrypt password
        String password = "secret"; 
        
        Instance inst = new ZooKeeperInstance(instanceName, zooServers);
        Connector conn = inst.getConnector(userName, password);
        
        return conn;
    }
    
    public void importRFileIntoAccumulo(String tableName, String inputDirectory, String failedDirectory) throws Exception
    {

        TableOperations ops = connectorBuilder().tableOperations();
        
        if (ops.exists(tableName))
        {
            ops.delete(tableName);
        }
        ops.create(tableName);
        
        //System.out.println(inputDirectory);
        //System.out.println(failedDirectory);
        ops.importDirectory(tableName, inputDirectory, failedDirectory, false);
        ops.compact(tableName, null, null, true, false);
    }
}
