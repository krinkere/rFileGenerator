/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.blogpost.alkrinker.rfilegenerator;

import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author 560651
 */
public class GenerateTest extends TestCase {
    
    public GenerateTest(String testName) {
        super(testName);
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test of main method, of class Generate.
     */
    public void testMain() {
        System.out.println("main");
        String[] args = null;
        Generate.main(args);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of writeFile method, of class Generate.
     */
    public void testWriteFile() throws Exception {
        System.out.println("writeFile");
        Path expResult = null;
        Path result = Generate.writeFile();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}
