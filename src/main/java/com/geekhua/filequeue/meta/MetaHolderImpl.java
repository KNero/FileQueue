package com.geekhua.filequeue.meta;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Leo Liang
 * 
 */
public class MetaHolderImpl implements MetaHolder 
{
	private static final Logger log = LoggerFactory.getLogger(MetaHolderImpl.class);
	private static final String METAFILE_DIRNAME = "meta";
	private static final String METAFILE_NAME = "meta";
	private static final int METAFILE_SIZE = 200;
	private static final byte[] BUF_MASK = new byte[METAFILE_SIZE];
	private static final byte[] NEW_LINE = "\n".getBytes();

	private AtomicReference<Meta> meta;
	private File baseDir;
	
	private RandomAccessFile randomFile;
	private MappedByteBuffer mbb;

	public MetaHolderImpl(String _name, String _baseDir) 
	{
		this.baseDir = new File(new File(_baseDir, _name), METAFILE_DIRNAME);
	}

	public void update(long _readingFileNo, long _readingFileOffset) 
	{
		this.meta.set(new Meta(_readingFileNo, _readingFileOffset));
		
		this._saveToFile(_readingFileNo, _readingFileOffset);
	}

	public void init() throws IOException 
	{
		this._createFileIfNeed();
		this._loadFromFile();
	}

	private void _createFileIfNeed() throws IOException 
	{
		if(! this.baseDir.exists()) 
		{
			this.baseDir.mkdirs();
		}

		File f = this._getMetaFile();
		if(! f.exists()) 
		{
			f.createNewFile();
		}
	}

	private File _getMetaFile() 
	{
		return new File(baseDir, METAFILE_NAME);
	}

	private synchronized void _saveToFile(long readingFileNo, long readingFileOffset) 
	{
		this.mbb.position(0);
		this.mbb.put(BUF_MASK);
		this.mbb.position(0);
		this.mbb.put(String.valueOf(readingFileNo).getBytes());
		this.mbb.put(NEW_LINE);
		this.mbb.put(String.valueOf(readingFileOffset).getBytes());
		this.mbb.put(NEW_LINE);
	}

	private void _loadFromFile() throws IOException 
	{
		File f = this._getMetaFile();

		FileReader fr = null;
		BufferedReader br = null;

		try 
		{
			fr = new FileReader(f);
			br = new BufferedReader(fr);
			String readingFileNoStr = br.readLine();
			String readingFileOffsetStr = br.readLine();
			
			long readingFileNo = StringUtils.isNumeric(readingFileNoStr) ? Long.valueOf(readingFileNoStr) : -1L;
			long readingFileOffset = StringUtils.isNumeric(readingFileOffsetStr) ? Long.valueOf(readingFileOffsetStr) : 0L;
			Meta metaInfo = new Meta(readingFileNo, readingFileOffset);
			this.meta = new AtomicReference<Meta>(metaInfo);
			
			this.randomFile = new RandomAccessFile(f, "rwd");
			this.mbb = this.randomFile.getChannel().map(MapMode.READ_WRITE, 0, METAFILE_SIZE);
		}
		finally 
		{
			if(fr != null) 
			{
				try
				{
					fr.close();
				}
				catch(IOException e) 
				{
					log.warn("Close meta file fail.");
				}
			}
			
			if(br != null) 
			{
				try 
				{
					br.close();
				} 
				catch(IOException e) 
				{
					log.warn("Close meta file fail.");
				}
			}
		}

	}

	public long getReadingFileNo() 
	{
		return meta.get().getReadingFileNo();
	}

	public long getReadingFileOffset() 
	{
		return meta.get().getReadingFileOffset();
	}

	public void close() throws IOException
	{
		this.randomFile.close();
	}
}
