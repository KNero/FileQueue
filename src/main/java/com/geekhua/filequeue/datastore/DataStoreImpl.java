package com.geekhua.filequeue.datastore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.geekhua.filequeue.Config;
import com.geekhua.filequeue.codec.Codec;

/**
 * 
 * @author Leo Liang
 * 
 */
public class DataStoreImpl<E> implements DataStore<E> 
{
	private static final String DATAFILE_DIRNAME = "data";
	private static final String DATAFILE_PREFIX = "fdata-";
	private static final String DATAFILE_SUFIX = ".fq";
	private static final String DATAFILE_BAKDIR = "bak";

	private static final Logger log = LoggerFactory.getLogger(DataStoreImpl.class);

	private static final byte[] DATAFILE_END_CONTENT = 
			new byte[] {(byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAB };

	private byte[] DATAFILE_END;
	private File baseDir;
	private File bakDir;
	private int blockSize;
	private Codec codec;
	private long fileSize;
	private String name;
	private RandomAccessFile readingFile = null;
	private AtomicLong readingFileNo = new AtomicLong(-1L);
	private AtomicLong readingOffset = new AtomicLong(0L);
	private boolean bakReadFile = false;

	private AtomicLong writingFileNo = new AtomicLong(-1L);
	private RandomAccessFile writingFile = null;

	public DataStoreImpl(Config config) 
	{
		this.name = config.getName();
		this.baseDir = new File(new File(config.getBaseDir(), name), DATAFILE_DIRNAME);
		this.blockSize = BlockGroup.estimateBlockSize(config.getMsgAvgLen());
		this.readingFileNo = new AtomicLong(config.getReadingFileNo());
		this.readingOffset = new AtomicLong(config.getReadingOffset());
		this.codec = config.getCodec();
		this.fileSize = config.getFileSize();
		this.bakReadFile = config.isBakReadFile();
		this.bakDir = new File(new File(config.getBaseDir(), name), DATAFILE_BAKDIR);
	}

	public void init() throws IOException {
		BlockGroup endFileBlockGroup = BlockGroup.allocate(DATAFILE_END_CONTENT.length, blockSize);
		endFileBlockGroup.setContent(DATAFILE_END_CONTENT);

		this.DATAFILE_END = endFileBlockGroup.array();

		this._createBaseDirIfNeeded();
		this._createBakDirIfNeeded();
		this._getLastDataFileNo();
		this._recoverLastDataFileIfNeeded();
		this._createNewWriteFile();
		
		checkReadingFile();
		_openReadingFile();
	}

	private boolean _createBaseDirIfNeeded() 
	{
		if(!this.baseDir.exists()) 
		{
			return this.baseDir.mkdirs();
		}

		return true;
	}

	private boolean _createBakDirIfNeeded() 
	{
		if(!this.bakDir.exists()) 
		{
			return bakDir.mkdirs();
		}

		return true;
	}

	private void _createNewWriteFile() throws IOException 
	{
		if(this.writingFile != null) 
		{
			try 
			{
				this.writingFile.write(DATAFILE_END);
				this.writingFile.close();
			}
			catch(IOException e) 
			{
				throw e;
			}

			this.writingFile = null;
		}

		String newWriteFileName = this.getNewWriteFileName();
		if(StringUtils.isNotBlank(newWriteFileName)) 
		{
			try 
			{
				this.writingFile = new RandomAccessFile(new File(baseDir, newWriteFileName), "rw");
			}
			catch(FileNotFoundException e) 
			{
				throw new IllegalStateException(String.format("File(%s) not found", newWriteFileName), e);
			}
		} 
		else 
		{
			throw new IllegalStateException("Can not create new write file");
		}
	}

	private long _getFileNumber(String _fileName) {
		String fileNumber = _fileName.substring(DATAFILE_PREFIX.length(),
				_fileName.length() - DATAFILE_SUFIX.length());
		return StringUtils.isBlank(_fileName) ? 0L : Long.valueOf(fileNumber);
	}

	public String getNewWriteFileName() {
		return getDataFileName(writingFileNo.incrementAndGet());
	}

	public String getDataFileName(long fileNo) 
	{
		return DATAFILE_PREFIX + String.format("%018d", fileNo)	+ DATAFILE_SUFIX;
	}

	private void checkReadingFile() {
		if (readingFileNo.longValue() < 0) {
			readingFileNo = new AtomicLong(0);
			readingOffset.set(0L);
		}

		File file = new File(baseDir,
				getDataFileName(readingFileNo.longValue()));
		while (!file.exists()) {
			file = new File(baseDir,
					getDataFileName(readingFileNo.incrementAndGet()));
			readingOffset.set(0L);
		}

	}

	private void _recoverLastDataFileIfNeeded() throws IOException {
		if (writingFileNo.longValue() >= 0) {
			RandomAccessFile lastWriteFile = null;

			try {
				String fileName = this.getDataFileName(writingFileNo
						.longValue());
				lastWriteFile = new RandomAccessFile(
						new File(baseDir, fileName), "rw");

				if (lastWriteFile.length() % this.blockSize != 0) {
					long newLength = (lastWriteFile.length() / this.blockSize + 1)
							* blockSize;
					lastWriteFile.seek(newLength);
					lastWriteFile.setLength(newLength);
				} else {
					lastWriteFile.seek(lastWriteFile.length());
				}

				lastWriteFile.write(DATAFILE_END);
			} finally {
				if (lastWriteFile != null) {
					lastWriteFile.close();
				}
			}

		}
	}

	private void _getLastDataFileNo() 
	{
		String[] dataFilesArr = baseDir.list(new FilenameFilter() {
			public boolean accept(File dir, String name) 
			{
				if (StringUtils.endsWith(name, DATAFILE_SUFIX) && StringUtils.startsWith(name, DATAFILE_PREFIX)) 
				{
					return true;
				}

				return false;
			}
		});

		long maxDataFileNo = -1L;

		for (String dataFile : dataFilesArr) 
		{
			long fileNo = _getFileNumber(dataFile);
			if(fileNo > maxDataFileNo) 
			{
				maxDataFileNo = fileNo;
			}
		}

		this.writingFileNo.set(maxDataFileNo);
	}

	private void _openReadingFile() 
	{
		if(this.readingFileNo.longValue() < 0 && this.writingFileNo.longValue() >= 0) 
		{
			this.readingFileNo = new AtomicLong(writingFileNo.longValue());
			this.readingOffset.set(0L);
		}
		
		if(this.readingFileNo.longValue() >= 0) 
		{
			try 
			{
				String fileName = this.getDataFileName(this.readingFileNo.longValue());
				this.readingFile = new RandomAccessFile(new File(baseDir, fileName), "r");

				long readPosition = readingOffset.longValue();
				if(readPosition > 0L) 
				{
					readingFile.seek(readPosition % blockSize == 0 ? readPosition : (readPosition / blockSize + 1) * blockSize);
				}
			} 
			catch(IOException e) 
			{
				if(this.readingFile != null) 
				{
					try
					{
						this.readingFile.close();
						this.readingFile = null;
					} 
					catch(Exception e1) 
					{
						// ignore
					}
				}
				
				String fileName = this.getDataFileName(this.readingFileNo.longValue());
				throw new IllegalStateException(String.format("File(%s) open fail",	fileName), e);
			}
		}
	}

	public void put(E element) throws IOException 
	{
		byte[] content = codec.encode(element);

		if (content != null && content.length != 0) 
		{
			// 설정한 파일사이즈보다 새로운 파일을 생성
			if(this.writingFile.length() >= this.fileSize) 
			{
				this._createNewWriteFile();
			}

			BlockGroup blockGroup = BlockGroup.allocate(content.length, this.blockSize);
			blockGroup.setContent(content);

			if(this.writingFile.length() % this.blockSize != 0) 
			{
				this.writingFile.seek((this.writingFile.length() / blockSize + 1) * blockSize);
			}
			
			this.writingFile.write(blockGroup.array());
		}
	}

	@SuppressWarnings("unchecked")
	public E take() throws IOException 
	{
		BlockGroup blockGroup = null;

		if(this.readingFileNo.longValue() >= 0) 
		{
			if(this.readingFile == null) 
			{
				this._openReadingFile();
			}

			blockGroup = BlockGroup.read(this.readingFile, this.blockSize);
			
			if((blockGroup != null && ArrayUtils.isEquals(blockGroup.array(), DATAFILE_END)) || blockGroup == null) 
			{
				if(this.readingFileNo.longValue() < writingFileNo.longValue()) 
				{
					if(readingFile != null) 
					{
						this.readingFile.close();
						this.readingFile = null;
						
						if(this.bakReadFile) 
						{
							try 
							{
								String fileName = this.getDataFileName(this.readingFileNo.longValue());
								FileUtils.moveFileToDirectory(new File(baseDir, fileName), bakDir, true);
							} 
							catch(IOException e) 
							{
								String fileName = this.getDataFileName(this.readingFileNo.longValue());
								log.warn("Move file({}) to dir({}) fail.", new File(baseDir, fileName), bakDir);
							}
						} 
						else 
						{
							String fileName = this.getDataFileName(this.readingFileNo.longValue());
							FileUtils.deleteQuietly(new File(baseDir, fileName));
						}
					}
					
					this.readingFileNo.incrementAndGet();
					this.readingOffset.set(0L);
					
					this._openReadingFile();
					
					return take();
				}
			}
		}
		
		if(blockGroup == null) 
		{
			return null;
		}
		else
		{
			this.readingOffset.set(this.readingFile.getFilePointer());
			
			return (E)codec.decode(blockGroup.getContent());
		}

	}

	public long readingFileOffset() {
		return readingOffset.longValue();
	}

	public long readingFileNo() {
		return readingFileNo.longValue();
	}

	public void close() {
		if (readingFile != null) {
			try {
				readingFile.close();
			} catch (IOException e) {
				log.warn("Close reading file({}) fail.",
						getDataFileName(readingFileNo.longValue()));
			}
		}

		if (writingFile != null) {
			try {
				writingFile.close();
			} catch (IOException e) {
				log.warn("Close reading file({}) fail.",
						getDataFileName(writingFileNo.longValue()));
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.geekhua.filequeue.datastore.DataStore#writingFileNo()
	 */
	public long writingFileNo() {
		return writingFileNo.get();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.geekhua.filequeue.datastore.DataStore#writingFileOffset()
	 */
	public long writingFileOffset() {
		try {
			return writingFile.getFilePointer();
		} catch (IOException e) {
			return -1;
		}
	}

}
