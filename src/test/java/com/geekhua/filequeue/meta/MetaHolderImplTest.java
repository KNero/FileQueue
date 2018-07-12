package com.geekhua.filequeue.meta;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author knero
 * 
 */
public class MetaHolderImplTest {
    @Test
    public void testReadWrite() throws IOException {
        boolean isDelete = new File("target/fileque/test-meta/meta/meta").delete();
        Assert.assertTrue(isDelete);

        MetaHolderImpl metaHolder = new MetaHolderImpl("test-meta", "target/fileque");
        metaHolder.init();
        Assert.assertEquals(0, metaHolder.getReadingFileNo());
        Assert.assertEquals(0, metaHolder.getReadingFileOffset());

        metaHolder.update(10, 20);
        Assert.assertEquals(10, metaHolder.getReadingFileNo());
        Assert.assertEquals(20, metaHolder.getReadingFileOffset());

        metaHolder.close();

        //파일을 다시 열어서 그전 데이터가 있는지 확인
        metaHolder = new MetaHolderImpl("test-meta", "target/fileque");
        metaHolder.init();
        Assert.assertEquals(10, metaHolder.getReadingFileNo());
        Assert.assertEquals(20, metaHolder.getReadingFileOffset());

        metaHolder.update(20, 30);
        Assert.assertEquals(20, metaHolder.getReadingFileNo());
        Assert.assertEquals(30, metaHolder.getReadingFileOffset());

        metaHolder.close();
    }
}
