package com.geekhua.filequeue.datastore;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.geekhua.filequeue.utils.EncryptUtils;
import org.powermock.reflect.Whitebox;

/**
 * 
 * @author kwonsm
 * 
 */
public class BlockGroupTest {
    private static final File baseDir = new File("./target/fileque/blockGroupTest");
    private static final byte[] HEADER      = new byte[] { (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAB };
    private static final int CHECKSUM_LEN = 20;
	private static final int HEADER_LEN = 4;
	private static final int CONTENT_CHECKSUM_LEN = 4;

    @Before
    public void before() throws Exception {
        if (baseDir.exists()) {
            FileUtils.deleteDirectory(baseDir);
        }
        baseDir.mkdirs();
    }

    @After
    public void after() throws Exception {
        if (baseDir.exists()) {
            FileUtils.deleteDirectory(baseDir);
        }
    }

    @Test
    public void test_allocate() throws Exception {
    	int blockSize = 3;
        byte[] content = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        BlockGroup blockGroup = BlockGroup.allocate(content, blockSize);

		byte[] buf = blockGroup.array();

		boolean checkHeader = Whitebox.invokeMethod(BlockGroup.class, "_validateHeader", buf);
		Assert.assertTrue(checkHeader);

		ByteBuffer temp = ByteBuffer.wrap(buf);
		temp.getInt();
		int contentAndChecksumLen = temp.getInt();
		boolean checksum = Whitebox.invokeMethod(BlockGroup.class, "_validateChecksum", buf, contentAndChecksumLen);
		Assert.assertTrue(checksum);

		byte[] copyContent = new byte[content.length];
		temp.get(copyContent);
		Assert.assertArrayEquals(content, copyContent);
    }

    @Test
    public void test_readBlockGroup() throws Exception {
        byte[] content = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
		RandomAccessFile file = null;

        try {
			BlockGroup blockGroup = BlockGroup.allocate(content, 3);

			file = getFile();
			file.write(blockGroup.array());
			file.seek(0);

			BlockGroup readBlockGroup = BlockGroup.read(file, blockGroup.getBlockSize());
			Assert.assertArrayEquals(content, readBlockGroup.getContent());
		} finally {
        	if (file != null) {
				file.close();
			}
		}
    }

    @Test
    public void test_readNextBlockAfterHeaderFail() throws Exception {
    	int blockSize = 1024;
		RandomAccessFile file = null;

		try {
			file = getFile();

			ByteBuffer buf = ByteBuffer.allocate(blockSize);
			buf.put(new byte[]{'t', 'e', 's', 't'});
			buf.putInt(blockSize - HEADER_LEN);

			while (buf.remaining() > blockSize) {
				buf.put((byte) 0);
			}

			file.write(buf.array());

			byte[] content = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
			BlockGroup blockGroup = BlockGroup.allocate(content, blockSize);

			file.write(blockGroup.array());
			file.seek(0);

			BlockGroup readBlockGroup = BlockGroup.read(file, 1024);
			Assert.assertArrayEquals(content, readBlockGroup.getContent());
		} finally {
			if (file != null) {
				file.close();
			}
		}
    }

    @Test
    public void test_readNextBlockAfterChecksumFail() throws Exception {
		int blockSize = 1024;
		RandomAccessFile file = null;

		try {
			file = getFile();

			ByteBuffer buf = ByteBuffer.allocate(blockSize);
			buf.put(HEADER);
			buf.putInt(blockSize - HEADER_LEN);

			for (int i = 0; i < 3; i++) {
				buf.put(new byte[]{'t', 'e', 's', 't'});
			}

			while (buf.remaining() > 0) {
				buf.put((byte) 0);
			}

			file.write(buf.array());

			byte[] content = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
			BlockGroup blockGroup = BlockGroup.allocate(content, blockSize);

			file.write(blockGroup.array());
			file.seek(0);

			BlockGroup readBlockGroup = BlockGroup.read(file, blockSize);
			Assert.assertArrayEquals(content, readBlockGroup.getContent());
		} finally {
			if (file != null) {
				file.close();
			}
		}
    }

    @Test
    public void test_readBlockGroupFailByCheckSumMultiBlocks() throws Exception {
		RandomAccessFile file = null;
		int blockSize = 100;

		try {
			file = getFile();

			ByteBuffer blocks = ByteBuffer.allocate(blockSize * 6);
			for (int i = 0; i < 3; i++) {
				blocks.put(new byte[] {34, 5, 5, 66});
			}

			while (blocks.remaining() > 0) {
				blocks.put((byte) 0);
			}

			file.write(blocks.array());

			byte[] content = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
			BlockGroup blockGroup = BlockGroup.allocate(content, blockSize);

			file.write(blockGroup.array());
			file.seek(0);

			BlockGroup readBlockGroup = BlockGroup.read(file, blockSize);
			Assert.assertArrayEquals(content, readBlockGroup.getContent());
		} finally {
			if (file != null) {
				file.close();
			}
		}
    }

    @Test
    public void test_readBlockGroupWithFlushDelay() throws Exception {
		RandomAccessFile file = null;
		int blockSize = 50;

		try {
			file = getFile();

			ByteBuffer blocks = ByteBuffer.allocate(blockSize * 6);
			for (int i = 0; i < 3; i++) {
				blocks.put(new byte[] {34, 5, 5, 66});
			}

			while (blocks.remaining() > 0) {
				blocks.put((byte) 0);
			}

			file.write(blocks.array());

			byte[] content = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
			BlockGroup blockGroup = BlockGroup.allocate(content, blockSize);

			file.write(blockGroup.array());

			blocks = ByteBuffer.allocate(blockSize * 2);
			for (int i = 0; i < 3; i++) {
				blocks.put(new byte[] {34, 5, 5, 66});
			}

			while (blocks.remaining() > 0) {
				blocks.put((byte) 0);
			}

			file.write(blocks.array());
			file.seek(0);

			BlockGroup readBlockGroup = BlockGroup.read(file, blockSize);
			Assert.assertArrayEquals(content, readBlockGroup.getContent());
			Assert.assertNotSame(file.length(), file.getFilePointer());
		} finally {
			if (file != null) {
				file.close();
			}
		}
    }

    private RandomAccessFile getFile() throws Exception {
        return new RandomAccessFile(new File(baseDir, "test"), "rw");
    }
}
