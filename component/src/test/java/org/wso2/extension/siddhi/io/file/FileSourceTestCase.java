package org.wso2.extension.siddhi.io.file;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;

import java.util.concurrent.atomic.AtomicInteger;

public class FileSourceTestCase {
    private static final Logger log = Logger.getLogger(FileSourceTestCase.class);
    private AtomicInteger count = new AtomicInteger();
    private AtomicInteger count1 = new AtomicInteger();
    private boolean paused;

    //  current state and restore state

    @Test
    public void testJsonInputDefaultMappingwithpaused() throws InterruptedException {
        log.info("______________Test to read the files and their contents under the given folder______________");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', folder.path='Files', file.extension='json', @map(type='json')) " +
                "define stream FooStream (symbol string,  price float,  volume int); " +
                "define stream BarStream (symbol string,  price float,  volume int); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                //EventPrinter.print(events);
                for (Event event : events) {
                    int n = count.getAndIncrement();
                    log.info("count" + count);
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals("ATM", event.getData(0));
                            break;
                        case 1:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("wso2", event.getData(0));
                            break;
                        case 4:
                            AssertJUnit.assertEquals("qwe", event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals("ert", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        File f = new File("Files/text.json");
        if (f.exists())
        {
            //delete if exists
            f.delete();
        }

        try {
            //File f = new File("Files/text.json");
            FileWriter fw = new FileWriter(f, true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("[{\"event\":{\"symbol\":\"ATM\", \"price\":55.6, \"volume\":100}}, ");
            bw.newLine();
            bw.write("{\"event\":{\"symbol\":\"IBM\", \"price\":55.6, \"volume\":200}}, ");
            bw.newLine();
            bw.write("{\"event\":{\"symbol\":\"IBM\", \"price\":55.6, \"volume\":300}}]");
            //byte[] snapshot = siddhiAppRuntime.snapshot();
            bw.newLine();
            bw.flush();

        } catch (IOException e) {
            log.info("error" , e);

        }
        log.info("Starting runtime for the 1st time.");
        siddhiAppRuntime.start();

        Thread.sleep(1000);
        //Getting snapshot
        byte[] snapshot = siddhiAppRuntime.snapshot();
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
        //assert event count
        //SiddhiTestHelper.waitForEvents(2000, 8, count, 30000);
        AssertJUnit.assertEquals("Number of events", 3 , count.get());

        try {
            //File f = new File("Files/text.json");
            FileWriter fw = new FileWriter(f, true);
            BufferedWriter bw = new BufferedWriter(fw);
            LineNumberReader  lnr = new LineNumberReader(new FileReader(f));
            for (int i  = 1; i < lnr.getLineNumber(); i++) {
                bw.newLine();
            }
            bw.write("[{\"event\":{\"symbol\":\"wso2\", \"price\":55.6, \"volume\":400}}, ");
            bw.newLine();
            bw.write("{\"event\":{\"symbol\":\"qwe\", \"price\":55.6, \"volume\":500}}, ");
            bw.newLine();
            bw.write("{\"event\":{\"symbol\":\"ert\", \"price\":55.6, \"volume\":600}}]");
            //byte[] snapshot = siddhiAppRuntime.snapshot();
            bw.newLine();
            bw.flush();
            lnr.close();
        } catch (IOException e) {
            log.info("error" , e);

        }


        // Restoring snapshot
        siddhiAppRuntime.restore(snapshot);
        log.info(count);
        // Restarting runtime
        siddhiAppRuntime.start();
        //SiddhiTestHelper.waitForEvents(2000, 10, count, 30000);
        //assert event count
         AssertJUnit.assertEquals("Number of events", 6, count.get());
        // Siddhi app shutdown
        siddhiAppRuntime.shutdown();
    }

    // in this case count increase after the restart Siddhi App runtime. but
    @Test
    public void testJsonInputDefaultMappingwithpaused11() throws InterruptedException {
        log.info("______________Test to read the files and their contents under the given folder______________");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', folder.path='Files', file.extension='json', @map(type='json')) " +
                "define stream FooStream (symbol string,  price float,  volume int); " +
                "define stream BarStream (symbol string,  price float,  volume int); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                //EventPrinter.print(events);
                for (Event event : events) {
                    int n = count.getAndIncrement();
                    log.info("count" + count);
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        case 1:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals("ATM", event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 7:
                            AssertJUnit.assertEquals("ATM", event.getData(0));
                            break;
                        case 8:
                            AssertJUnit.assertEquals("ATM", event.getData(0));
                            break;
                        case 9:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        File f = new File("Files/text.json");
        if (f.exists())
        {
            //delete if exists
            f.delete();
        }

        try {
            //File f = new File("Files/text.json");
            FileWriter fw = new FileWriter(f, true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("[{\"event\":{\"symbol\":\"ATM\", \"price\":55.6, \"volume\":100}}, ");
            bw.newLine();
            bw.write("{\"event\":{\"symbol\":\"IBM\", \"price\":55.6, \"volume\":200}}, ");
            bw.newLine();
            bw.write("{\"event\":{\"symbol\":\"IBM\", \"price\":55.6, \"volume\":300}}]");
            //byte[] snapshot = siddhiAppRuntime.snapshot();
            bw.newLine();
            bw.flush();

        } catch (IOException e) {
            log.info("error" , e);

        }
        log.info("Starting runtime for the 1st time.");
        siddhiAppRuntime.start();

        Thread.sleep(1000);
        //Getting snapshot
        byte[] snapshot = siddhiAppRuntime.snapshot();
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
        //assert event count
        //SiddhiTestHelper.waitForEvents(2000, 8, count, 30000);
        AssertJUnit.assertEquals("Number of events", 4 , count.get());
        SiddhiManager siddhiManager1= new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime1.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                //EventPrinter.print(events);
                for (Event event : events) {
                    int n = count.getAndIncrement();
                    log.info("count" + count);
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        case 1:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals("WSO2#@$", event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals("WSO2#@$", event.getData(0));
                            break;
                        case 7:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 8:
                            AssertJUnit.assertEquals("wso2", event.getData(0));
                            break;
                        case 9:
                            AssertJUnit.assertEquals("qwe", event.getData(0));
                            break;
                        case 10:
                            AssertJUnit.assertEquals("ert", event.getData(0));
                            break;
                        case 11:
                            AssertJUnit.assertEquals("ert", event.getData(0));
                            break;
                        case 12:
                            AssertJUnit.assertEquals("ert", event.getData(0));
                            break;
                        case 13:
                            AssertJUnit.assertEquals("ert", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        try {
            //File f = new File("Files/text.json");
            FileWriter fw = new FileWriter(f, true);
            BufferedWriter bw = new BufferedWriter(fw);
            LineNumberReader  lnr = new LineNumberReader(new FileReader(f));
            for (int i  = 1; i < lnr.getLineNumber(); i++) {
                bw.newLine();
            }
            bw.write("[{\"event\":{\"symbol\":\"wso2\", \"price\":55.6, \"volume\":400}}, ");
            bw.newLine();
            bw.write("{\"event\":{\"symbol\":\"qwe\", \"price\":55.6, \"volume\":500}}, ");
            bw.newLine();
            bw.write("{\"event\":{\"symbol\":\"ert\", \"price\":55.6, \"volume\":600}},");
            bw.newLine();
            bw.write("{\"event\":{\"symbol\":\"ert\", \"price\":55.6, \"volume\":600}},");
            bw.newLine();
            bw.write("{\"event\":{\"symbol\":\"ert\", \"price\":55.6, \"volume\":600}},");
            bw.newLine();
            bw.write("{\"event\":{\"symbol\":\"ert\", \"price\":55.6, \"volume\":600}}]");
            //byte[] snapshot = siddhiAppRuntime.snapshot();
            bw.newLine();
            bw.flush();
            lnr.close();
        } catch (IOException e) {
            log.info("error" , e);

        }


        // Restoring snapshot
        siddhiAppRuntime1.restore(snapshot);
        log.info("check");
        log.info(count);
        // Restarting runtime
        siddhiAppRuntime1.start();



        //SiddhiTestHelper.waitForEvents(2000, 10, count, 30000);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 14, count.get());
        // Siddhi app shutdown
        siddhiAppRuntime1.shutdown();
    }
}
