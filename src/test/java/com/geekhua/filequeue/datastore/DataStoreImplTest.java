package com.geekhua.filequeue.datastore;

import com.geekhua.filequeue.Config;
import com.geekhua.filequeue.codec.ByteArrayCodec;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataStoreImplTest {
    private static final File   baseDir = new File("target/fileque", "data-store-test");

    @Before
    public void before() throws Exception {
        if (baseDir.exists()) {
            FileUtils.deleteDirectory(baseDir);
        }
        Assert.assertTrue(baseDir.mkdirs());
    }

    @Test
    public void testStringPutAndTake() throws Exception {
        Config config = new Config();
        config.setBaseDir(baseDir.getAbsolutePath());

        DataStore<String> ds = new DataStoreImpl<>(config, 0, 0);
        try {
	        ds.init();

	        String content = "0123456789";
	        for (int i = 0; i < 10; ++i) {
		        ds.put(content);
	        }

	        for (int i = 0; i < 10; ++i) {
		        String buf = ds.take();
		        Assert.assertEquals(content, buf);
	        }
        } finally {
	        ds.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
	public void testMapPutAndTake() throws Exception {
	    Config config = new Config();
	    config.setBaseDir(baseDir.getAbsolutePath());

	    DataStore<Map> ds = new DataStoreImpl<>(config, 0, 0);
	    try {
		    ds.init();

		    List<String> list = new ArrayList<>();
		    list.add("a");
		    list.add("b");

		    Map<String, Object> data = new HashMap<>();
		    data.put("String", "String");
		    data.put("Integer", 1234);
		    data.put("List", list);

		    ds.put(data);
		    Map<String, Object> take = ds.take();

		    Assert.assertEquals(data.get("String"), take.get("String"));
		    Assert.assertEquals(data.get("Integer"), take.get("Integer"));
		    List takeList = (List) take.get("List");
		    Assert.assertEquals(list, takeList);
	    } finally {
		    ds.close();
	    }
    }

    @Test
	public void testCreateNewWriteFileAndBackup() throws Exception {
	    Config config = new Config();
	    config.setBaseDir(baseDir.getAbsolutePath());
	    config.setCodec(new ByteArrayCodec());
	    config.setMsgAvgLen(10);
	    config.setFileSiz(100);
	    config.setBackupReadFile(true);
	    byte[] data = "18573957538474".getBytes();

	    DataStore<byte[]> ds = new DataStoreImpl<>(config, 0, 0);
	    try {
		    ds.init();

		    for (int i = 0; i < 20; ++i) {
			    ds.put(data);
		    }
	    } finally {
		    ds.close();
	    }

	    ds = new DataStoreImpl<>(config, 0, 0);
	    try {
		    ds.init();

		    for (int i = 0; i < 20; ++i) {
			    Assert.assertTrue(Arrays.equals(data, ds.take()));
		    }

		    Assert.assertNull(ds.take());

		    data = "1023829874837483742".getBytes();
		    ds.put(data);
		    Assert.assertTrue(Arrays.equals(data, ds.take()));
		    Assert.assertEquals(ds.readingFileNo(), ds.writingFileNo());
		    Assert.assertEquals(ds.readingFileOffset(), ds.writingFileOffset());
	    } finally {
		    ds.close();
	    }
    }

	@Test
	public void testCreateNewWriteFileAndNoBackup() throws Exception {
		Config config = new Config();
		config.setBaseDir(baseDir.getAbsolutePath());
		config.setCodec(new ByteArrayCodec());
		config.setMsgAvgLen(10);
		config.setFileSiz(100);

		DataStore<byte[]> ds = new DataStoreImpl<>(config, 0, 0);
		try {
			ds.init();

			byte[] data = "18573957538474".getBytes();

			for (int i = 0; i < 20; ++i) {
				ds.put(data);
			}

			for (int i = 0; i < 20; ++i) {
				Assert.assertTrue(Arrays.equals(data, ds.take()));
			}
		} finally {
			ds.close();
		}
	}

	@Test(expected = IOException.class)
	public void testFailInit() throws Exception {
    	Config config = new Config();
		config.setBaseDir(baseDir.getAbsolutePath());
		config.setMsgAvgLen(20);
		config.setFileSiz(200);
		String data = "123456";

		DataStore<String> ds = new DataStoreImpl<>(config, 0, 0);
		try {
			ds.init();
			ds.put(data);
		} finally {
			ds.close();
		}

		// 메시지의 길이가 맞지 않으면 파일을 읽을 수 없기 때문에 초기화 과정에서 에러가 발생한다.
		config.setMsgAvgLen(10);

		ds = new DataStoreImpl<>(config, 0, 0);
		try {
			ds.init();
		} finally {
			ds.close();
		}
	}
}
