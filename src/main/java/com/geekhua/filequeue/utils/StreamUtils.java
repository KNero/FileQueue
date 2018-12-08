package com.geekhua.filequeue.utils;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Leo Liang
 * 
 */
public class StreamUtils {
    private StreamUtils() {

    }

	public static void readFully(RandomAccessFile file, byte[] buf, int offset, int length) throws IOException {
		if(length < 0) {
			throw new IndexOutOfBoundsException();
		}

		int n = 0;
		while(n < length) {
			int count = file.read(buf, offset + n, length - n);
			if(count < 0) {
				throw new EOFException("Can not read data. Expected to " + length + " bytes, read " + n + " bytes before inputstream close.");
			}
			n += count;
		}
	}
}
