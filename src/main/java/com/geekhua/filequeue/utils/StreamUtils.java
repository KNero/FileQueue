package com.geekhua.filequeue.utils;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Leo Liang
 * 
 */
public class StreamUtils {
	public static int readFully(RandomAccessFile _file, byte[] _b, int _off, int _len) throws IOException 
	{
		if(_len < 0) 
		{
			throw new IndexOutOfBoundsException();
		}

		int n = 0;
		while(n < _len) 
		{
			int count = _file.read(_b, _off + n, _len - n);
			if(count < 0) 
			{
				throw new EOFException("Can not read data. Expected to " + _len + " bytes, read " + n + " bytes before inputstream close.");
			}

			n += count;
		}
		
		return n;
	}
}
