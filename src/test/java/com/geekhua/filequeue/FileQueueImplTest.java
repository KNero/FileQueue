package com.geekhua.filequeue;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.junit.Assert;
import org.junit.Test;

import com.geekhua.filequeue.codec.MyObject;
import com.geekhua.filequeue.exception.FileQueueClosedException;


public class FileQueueImplTest {
    private static final File baseDir = new File("target/fileque");

    @Test
    public void testAdd() throws Exception {
        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setMsgAvgLen(10);
        config.setName("testAdd");
        FileQueue<String> fq = new FileQueueImpl<>(config);
        fq.add("ssss");
        Assert.assertEquals("ssss", fq.get());
        fq.close();
    }

    @Test
    public void testAddObj() throws Exception {
        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setMsgAvgLen(10);
        config.setName("testAddObj");
        FileQueue<MyObject> fq = new FileQueueImpl<>(config);
        for (int i = 0; i < 10000; i++) {
            fq.add(new MyObject());
        }
        fq.close();
    }
    @Test
    public void testAddObjMultThread() throws Exception {
    	Config config = new Config();
    	config.setBaseDir(baseDir.getAbsolutePath());
    	config.setMsgAvgLen(10);
    	config.setName("testAddObjMultThread");
    	final FileQueue<MyObject> fq = new FileQueueImpl<>(config);
    	ExecutorService executorService = Executors.newFixedThreadPool(20);
    	final int threads = 20;
    	final int max = 10000;
    	for (int i=0;i<threads;i++){
    		executorService.submit(new Runnable() {
				public void run() {
					for (int i = 0; i < max / threads; i++) {
						try {
							fq.add(new MyObject());
						} catch (IOException | FileQueueClosedException e) {
							e.printStackTrace();
						}
					}
				}
			});
    	}
    	executorService.shutdown();
    	executorService.awaitTermination(100, TimeUnit.SECONDS);
    	fq.close();
    }

    @Test
    public void testAddMultiFiles() throws Exception {
        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setMsgAvgLen(10);
        config.setName("testAddMultiFiles");
        config.setFileSiz(1024);
        FileQueue<Integer> fq = new FileQueueImpl<>(config);
        int times = 1000;
        for (int i = 0; i < times; i++) {
            fq.add(i);
        }

        for (int i = 0; i < times; i++) {
            Assert.assertEquals(Integer.valueOf(i), fq.get());
        }
        fq.close();
    }

    @Test
    public void testGetTimeout() throws Exception {
        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setMsgAvgLen(10);
        config.setName("testGetTimeout");
        config.setFileSiz(1024);
        FileQueue<Integer> fq = new FileQueueImpl<>(config);

        long start = System.currentTimeMillis();
        Integer res = fq.get(1, TimeUnit.SECONDS);
        Assert.assertEquals(1, (System.currentTimeMillis() - start) / 1000);
        Assert.assertNull(res);

    }

    @Test
    public void testQueueRestart() throws Exception {
        int times = 100;
        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setMsgAvgLen(10);
        config.setName("testQueueRestart");
        // single data file
        config.setFileSiz(1024 * 1024 * 1000);
        FileQueue<Integer> fq = new FileQueueImpl<>(config);
        for (int i = 0; i < times; i++) {
            fq.add(i);
        }

        for (int i = 0; i < times / 2; i++) {
            Assert.assertEquals(Integer.valueOf(i), fq.get());
        }

        fq.close();

        fq = new FileQueueImpl<>(config);
        for (int i = times / 2; i < times; i++) {
            Assert.assertEquals(Integer.valueOf(i), fq.get());
        }
    }

    @Test
    public void testQueueRestart2() throws Exception {
        int times = 1000;
        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setMsgAvgLen(10);
        config.setName("testQueueRestart2");
        // multi data files
        config.setFileSiz(10);
        FileQueue<Integer> fq = new FileQueueImpl<>(config);
        for (int i = 0; i < times; i++) {
            fq.add(i);
        }

        for (int i = 0; i < times / 2; i++) {
            Assert.assertEquals(Integer.valueOf(i), fq.get());
        }

        fq.close();
        Thread.sleep(5000);

        fq = new FileQueueImpl<>(config);
        for (int i = times / 2; i < times; i++) {
            Assert.assertEquals(Integer.valueOf(i), fq.get());
        }

    }

    @Test
    public void testWriteSpeed() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(1024);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(1024 * 1024 * 500);
        config.setName("testWriteSpeed");
        FileQueue<byte[]> fq = new FileQueueImpl<>(config);
        byte[] content = new byte[1024];
        for (int i = 0; i < 1024; i++) {
            content[i] = 0x55;
        }
        int times = 100000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            fq.add(content);
        }
        System.out.println("[Write]Time spend " + (System.currentTimeMillis() - start) + "ms for " + times
                + " times. Avg msg length 1024bytes, each data file 500MB.");

    }

    @Test
    public void testReadSpeed() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(1024);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(1024 * 1024 * 500);
        config.setName("testReadSpeed");
        
        FileQueue<byte[]> fq = new FileQueueImpl<>(config);
        byte[] content = new byte[1024];
        for (int i = 0; i < 1024; i++) {
            content[i] = 0x55;
        }

        int times = 100000;
        for (int i = 0; i < times; i++) {
            fq.add(content);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            fq.get();
        }
        System.out.println("[Read]Time spend " + (System.currentTimeMillis() - start) + "ms for " + times
                + " times. Avg msg length 1024bytes, each data file 500MB.");

    }

    @Test
    public void testReadWriteSpeed() throws Exception {
        Config config = new Config();
        config.setMsgAvgLen(1024);
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setFileSiz(1024 * 1024 * 500);
        config.setName("testReadWriteSpeed");
        
        FileQueue<byte[]> fq = new FileQueueImpl<>(config);
        byte[] content = new byte[1024];
        for (int i = 0; i < 1024; i++) {
            content[i] = 0x55;
        }

        int times = 100000;

        long start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            fq.add(content);
            fq.get();
        }
        System.out.println("[ReadWriteSpeed]Time spend " + (System.currentTimeMillis() - start) + "ms for " + times
                + " times. Avg msg length 1024bytes, each data file 500MB.");

    }

    @Test
    public void testReadWrite() throws Exception {
        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setName("testReadWrite");

        long start = System.currentTimeMillis();

        FileQueue<TestObject> fileQueue = new FileQueueImpl<>(config);
        for (int i = 0; i < 1000; ++i) {
            fileQueue.add(new TestObject(i, "name-" + i, true, i));
        }

        for (int i = 0; i < 1000; ++i) {
            TestObject testObject = fileQueue.get();

            Assert.assertEquals(i, testObject.count);
            Assert.assertEquals("name-" + i, testObject.name);
            Assert.assertEquals(i, testObject.pos);
        }

        System.out.println("[ReadWrite] elapsed: " + (System.currentTimeMillis() - start) + "ms.");
    }

    @Test
    public void concurrentTestReadFasterThanWrite() throws Exception {
        final int totalTimes = 10000;
        final int writerCount = 10;
        final int readerCount = 20;

        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setName("concurrentTestReadFasterThanWrite");

        final FileQueue<TestObject> fq = new FileQueueImpl<>(config);
        final Set<TestObject> results = Collections.synchronizedSet(new TreeSet<TestObject>());
        final Set<TestObject> expected = Collections.synchronizedSet(new TreeSet<TestObject>());

        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(writerCount + readerCount);

        for (int i = 0; i < writerCount; i++) {
            final int threadNum = i;
            Thread writerThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    for (int j = 0; j < totalTimes / writerCount; j++) {
                        try {
                            TestObject m = new TestObject(totalTimes, "t-" + threadNum + "-" + j, j % 2 == 0, j);
                            fq.add(m);
                            expected.add(m);
                            Thread.sleep(5);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    endLatch.countDown();
                }
            });

            writerThread.start();
        }

        for (int i = 0; i < readerCount; i++) {
            Thread readerThread = new Thread(new Runnable() {

                public void run() {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    for (int j = 0; j < totalTimes / readerCount; ++j) {
                        try {
                            TestObject m = fq.get(10, TimeUnit.SECONDS);
                            results.add(m);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    endLatch.countDown();
                }
            });

            readerThread.start();
        }

        startLatch.countDown();
        endLatch.await();

        Assert.assertEquals(expected.size(), results.size());
        Assert.assertEquals(expected, results);
    }

    @Test
    public void concurrentTestWriterFasterThanReader() throws Exception {
        final int totalTimes = 10000;
        final int writerCount = 20;
        final int readerCount = 10;

        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setName("concurrentTestWriterFasterThanReader");

        final FileQueue<TestObject> fq = new FileQueueImpl<TestObject>(config);
        final Set<TestObject> results = Collections.synchronizedSet(new TreeSet<TestObject>());

        final Set<TestObject> expected = Collections.synchronizedSet(new TreeSet<TestObject>());

        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(writerCount + readerCount);

        for (int i = 0; i < writerCount; i++) {
            final int threadNum = i;
            Thread writerThread = new Thread(new Runnable() {

                public void run() {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    for (int j = 0; j < totalTimes / writerCount; j++) {
                        try {
                            TestObject m = new TestObject(totalTimes, "t-" + threadNum + "-" + j, j % 2 == 0, j);
                            fq.add(m);
                            expected.add(m);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    endLatch.countDown();
                }
            });

            writerThread.start();
        }

        for (int i = 0; i < readerCount; i++) {
            Thread readerThread = new Thread(new Runnable() {

                public void run() {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    for (int j = 0; j < totalTimes / readerCount; j++) {
                        try {
                            TestObject m = fq.get(10, TimeUnit.SECONDS);
                            results.add(m);
                            Thread.sleep(5);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    endLatch.countDown();
                }
            });

            readerThread.start();
        }

        startLatch.countDown();
        endLatch.await();

        Assert.assertEquals(expected.size(), results.size());
        Assert.assertEquals(expected, results);
    }

    @Test
    public void concurrentTestWriterReaderWithSameSpeed() throws Exception {
        final int totalTimes = 10000;
        final int writerCount = 20;
        final int readerCount = 20;

        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setName("concurrentTestWriterReaderWithSameSpeed");

        final FileQueue<TestObject> fq = new FileQueueImpl<TestObject>(config);
        final Set<TestObject> results = Collections.synchronizedSet(new TreeSet<TestObject>());

        final Set<TestObject> expected = Collections.synchronizedSet(new TreeSet<TestObject>());

        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(writerCount + readerCount);

        for (int i = 0; i < writerCount; i++) {
            final int threadNum = i;
            Thread writerThread = new Thread(new Runnable() {

                public void run() {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    for (int j = 0; j < totalTimes / writerCount; j++) {
                        try {
                            TestObject m = new TestObject(totalTimes, "t-" + threadNum + "-" + j, j % 2 == 0, j);
                            fq.add(m);
                            expected.add(m);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    endLatch.countDown();
                }
            });

            writerThread.start();
        }

        for (int i = 0; i < readerCount; i++) {
            Thread readerThread = new Thread(new Runnable() {

                public void run() {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    for (int j = 0; j < totalTimes / readerCount; j++) {
                        try {
                            TestObject m = fq.get(10, TimeUnit.SECONDS);
                            results.add(m);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    endLatch.countDown();
                }
            });

            readerThread.start();
        }

        startLatch.countDown();
        endLatch.await();

        Assert.assertEquals(expected.size(), results.size());
        Assert.assertEquals(expected, results);
    }

    /**
     * 빈 큐파일을 닫고 열 경우 readingFileNo 가 업데이트 되지 않아
     * 다시 파일을 열 경우 계속해서 그 전 파일을 찾는 현상 테스트
     */
    @Test
    public void emptyFileClose() throws Exception {
        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setName("readEmptyFile");

        FileQueue<String> q;

        for (int i = 0; i < 5; ++i) {
            q = new FileQueueImpl<>(config);
            q.get();
            q.close();

            Thread.sleep(1000);
        }
    }

//    @Test
    public void stressTest() throws Exception {
        final int writerCount = 20;
        final int readerCount = 20;

        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());
        config.setName("stressTest");

        final FileQueue<TestObject> fq = new FileQueueImpl<TestObject>(config);

        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicBoolean exceptionOccur = new AtomicBoolean(false);

        for (int i = 0; i < writerCount; i++) {
            final int threadNum = i;
            Thread writerThread = new Thread(new Runnable() {

                public void run() {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    Random random = new Random(System.currentTimeMillis());

                    while (!exceptionOccur.get()) {
                        try {
                            TestObject m = new TestObject(1, "t-" + threadNum + "-" + 1, true, 1);
                            fq.add(m);
                            System.out.println("[Write]" + m);
                            Thread.sleep(random.nextInt(100));
                        } catch (Exception e) {
                            exceptionOccur.set(true);
                        }
                    }

                }
            });

            writerThread.start();
        }

        for (int i = 0; i < readerCount; i++) {
            Thread readerThread = new Thread(new Runnable() {

                public void run() {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    Random random = new Random(System.currentTimeMillis());

                    while (!exceptionOccur.get()) {
                        try {
                            TestObject m = fq.get();
                            System.out.println("[Read]" + m);
                            Thread.sleep(random.nextInt(100));
                        } catch (Exception e) {
                            exceptionOccur.set(true);
                        }
                    }

                }
            });

            readerThread.start();
        }

        startLatch.countDown();
        System.in.read();
    }

    public static class TestObject implements Comparable<TestObject>, Serializable {
        private static final long serialVersionUID = -5420562857827246766L;
        public int                count;
        public String             name;
        public boolean            enable;
        public long               pos;

        public TestObject(){
        	
        }
        public TestObject(int count, String name, boolean enable, long pos) {
            super();
            this.count = count;
            this.name = name;
            this.enable = enable;
            this.pos = pos;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + count;
            result = prime * result + (enable ? 1231 : 1237);
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            result = prime * result + (int) (pos ^ (pos >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TestObject other = (TestObject) obj;
            if (count != other.count)
                return false;
            if (enable != other.enable)
                return false;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            if (pos != other.pos)
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "TestObject [count=" + count + ", name=" + name + ", enable=" + enable + ", pos=" + pos + "]";
        }

        public int compareTo(TestObject o) {
            return CompareToBuilder.reflectionCompare(this, o);
        }

    }
}
