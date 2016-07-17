package com.geekhua.filequeue.meta;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Leo Liang
 * 
 */
public interface MetaHolder extends Closeable
{
	void update(long _readingFileNo, long _readingFileOffset);

	void init() throws IOException;

	long getReadingFileNo();

	long getReadingFileOffset();
	
	void close() throws IOException;
}
