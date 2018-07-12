package com.geekhua.filequeue.meta;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Leo Liang
 * 
 */
public class MetaHolderImpl implements MetaHolder {
	private static final String META_FILE_DIRNAME = "meta";
	private static final String META_FILE_NAME = "meta";
	private static final int META_FILE_SIZE = 16;

	private AtomicReference<Meta> meta;
	private File baseDir;
	
	private RandomAccessFile randomFile;
	private MappedByteBuffer fileMappedBuf;

	public MetaHolderImpl(String queueName, String baseDir) {
		this.baseDir = new File(new File(baseDir, queueName), META_FILE_DIRNAME);
	}

	public void update(long readingFileNo, long readingFileOffset) {
		this.meta.set(new Meta(readingFileNo, readingFileOffset));

		fileMappedBuf.position(0);
		fileMappedBuf.putLong(readingFileNo);
        fileMappedBuf.putLong(readingFileOffset);
	}

	public void init() throws IOException {
        if(!this.baseDir.exists() && !this.baseDir.mkdirs()) {
            throw new IOException("Can not create meta file directory.");
        }

		this.loadFromFile();
	}

	private void loadFromFile() throws IOException {
		File metaFile = new File(baseDir, META_FILE_NAME);
        randomFile = new RandomAccessFile(metaFile, "rwd");
        fileMappedBuf = randomFile.getChannel().map(MapMode.READ_WRITE, 0, META_FILE_SIZE);

        long readingFileNo = fileMappedBuf.getLong();
        long readingFileOffset = fileMappedBuf.getLong();
        meta = new AtomicReference<>(new Meta(readingFileNo, readingFileOffset));
	}

	public long getReadingFileNo() {
		return meta.get().getReadingFileNo();
	}

	public long getReadingFileOffset() 
	{
		return meta.get().getReadingFileOffset();
	}

	public void close() throws IOException {
		this.randomFile.close();
	}
}
