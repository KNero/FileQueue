package com.geekhua.filequeue.datastore;

import com.geekhua.filequeue.Config;
import com.geekhua.filequeue.codec.Codec;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.atomic.AtomicLong;

public class DataStoreImpl<E> implements DataStore<E> {
    private static final Logger log = LoggerFactory.getLogger(DataStoreImpl.class);

	private static final String DATAFILE_DIRNAME = "data";
	private static final String DATAFILE_PREFIX = "q-";
	private static final String DATAFILE_EXTENSION = ".fq";
	private static final String DATAFILE_BACK_DIR = "bak";

	private static final byte[] DATAFILE_END_CONTENT = 
			new byte[] {(byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAB};

	private File baseDir;
	private File backDir;
	private int blockSize;
	private Codec codec;
	private long maxFileSize;
	private String name;
	
	private RandomAccessFile readingFile = null;
	private AtomicLong readingFileNo;
	private AtomicLong readingOffset;
	private boolean bakReadFile;

	private AtomicLong writingFileNo = new AtomicLong(-1L);
	private RandomAccessFile writingFile = null;

	public DataStoreImpl(Config config) {
		this.name = config.getName();
		this.baseDir = new File(new File(config.getBaseDir(), name), DATAFILE_DIRNAME);
		this.blockSize = BlockGroup.estimateBlockSize(config.getMsgAvgLen());
		this.readingFileNo = new AtomicLong(config.getReadingFileNo());
		this.readingOffset = new AtomicLong(config.getReadingOffset());
		this.codec = config.getCodec();
		this.maxFileSize = config.getFileSize();
		this.bakReadFile = config.isBakReadFile();
		this.backDir = new File(new File(config.getBaseDir(), name), DATAFILE_BACK_DIR);
	}

    private static String getDataFileName(long fileNo) {
        return DATAFILE_PREFIX + String.format("%018d", fileNo)	+ DATAFILE_EXTENSION;
    }

    /**
     * @param fileName q-000000000000000001.fq 형식
     * @return 파일 큐 이름이 q-000000000000000001.fq 이면 1을 반환
     */
    private long getFileNumber(String fileName) {
        String fileNumber = fileName.substring(DATAFILE_PREFIX.length(), fileName.length() - DATAFILE_EXTENSION.length());
        return StringUtils.isBlank(fileName) ? 0L : Long.valueOf(fileNumber);
    }

	public void init() throws IOException {
		createBaseDirIfNeeded();
		createBakDirIfNeeded();
		initLastDataFileNo();
		recoverLastDataFileIfNeeded();
		createNewWriteFile();
		
		checkReadingFile();
		openReadingFile();
	}

	private void createBaseDirIfNeeded() {
		if (!baseDir.exists() || !baseDir.mkdirs()) {
            log.error("Can not create queue data directory. {}", baseDir.getAbsolutePath());
		}
	}

	private void createBakDirIfNeeded() {
		if (!backDir.exists() || !backDir.mkdirs()) {
            log.error("Can not create queue backup directory. {}", backDir.getAbsolutePath());
		}
	}

    /**
     * 큐파일 중에서 가장 마지막에 생성된 파일을 찾는다.
     */
    private void initLastDataFileNo() {
        String[] dataFilesArr = baseDir.list(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return StringUtils.endsWith(name, DATAFILE_EXTENSION) && StringUtils.startsWith(name, DATAFILE_PREFIX);
            }
        });

        long maxDataFileNo = -1L;

        if (dataFilesArr != null) {
            for (String dataFile : dataFilesArr) {
                long fileNo = getFileNumber(dataFile);
                if(fileNo > maxDataFileNo) {
                    maxDataFileNo = fileNo;
                }
            }

            this.writingFileNo.set(maxDataFileNo);
        }
    }

    /**
     * 전에 사용하던 마지막 큐파일은 더 이상 사용하지 않고 새로운 큐파일을 사용하기 위해 end block 을 쓴다.
     */
    private void recoverLastDataFileIfNeeded() throws IOException {
        if (writingFileNo.longValue() >= 0) {
            String fileName = getDataFileName(writingFileNo.longValue());

            File lastFile = new File(baseDir, fileName);
            if (lastFile.length() % blockSize != 0) {
                throw new IOException("file size % block size != 0. file size:" + lastFile.length() + ", block size:" + blockSize);
            }

            try (FileOutputStream lastWriteFile = new FileOutputStream(lastFile, true)) {
                BlockGroup endFileBlock = BlockGroup.allocate(DATAFILE_END_CONTENT, blockSize);
                lastWriteFile.write(endFileBlock.array());
            }
        }
    }

	private void createNewWriteFile() throws IOException {
        // init 단계에서는 null 이기 때문에 실행되지 않는다.
		if(this.writingFile != null) {
			//파일이 끝났다는 것을 표시한다.
            BlockGroup endFileBlock = BlockGroup.allocate(DATAFILE_END_CONTENT, blockSize);
			this.writingFile.write(endFileBlock.array());
			this.writingFile.close();
			this.writingFile = null;
		}

		String newWriteFileName = getDataFileName(this.writingFileNo.incrementAndGet());
        this.writingFile = new RandomAccessFile(new File(this.baseDir, newWriteFileName), "rw");
	}

	private void checkReadingFile() {
		if(readingFileNo.longValue() < 0) {
			readingFileNo = new AtomicLong(0);
			readingOffset.set(0L);
		}

		File file = new File(baseDir, getDataFileName(readingFileNo.longValue()));
		if (!file.exists()) {
			readingOffset.set(0L);
		}
	}

	private void openReadingFile() {
		if(this.readingFileNo.longValue() < 0 && this.writingFileNo.longValue() >= 0) {
			this.readingFileNo = new AtomicLong(writingFileNo.longValue());
			this.readingOffset.set(0L);
		}
		
		if(this.readingFileNo.longValue() >= 0) {
			try {
				String fileName = getDataFileName(this.readingFileNo.longValue());
				this.readingFile = new RandomAccessFile(new File(baseDir, fileName), "r");

				long readPosition = readingOffset.longValue();
				if(readPosition > 0L) {
					readingFile.seek(readPosition % blockSize == 0 ? readPosition : (readPosition / blockSize + 1) * blockSize);
				}
			} catch (IOException e) {
				if(this.readingFile != null) {
					try {
						this.readingFile.close();
						this.readingFile = null;
					} catch (Exception e1) {
						// ignore
					}
				}
				
				String fileName = getDataFileName(this.readingFileNo.longValue());
				throw new IllegalStateException(String.format("File(%s) open fail",	fileName), e);
			}
		}
	}

	public void put(E element) throws IOException {
		byte[] content = codec.encode(element);

		if(content != null && content.length != 0) {
			// 설정한 파일사이즈보다 새로운 파일을 생성
			if(this.writingFile.length() >= this.maxFileSize) {
				this.createNewWriteFile();
			}

			if(this.writingFile.length() % this.blockSize != 0) 
			{
				//만약 현재 파일크기가 설정된 block size와 나눠 떨어지지 않을 경우
				//나눠 떨어질 수 있는 다음 위치부터 파일을 기록한다.
				this.writingFile.seek((this.writingFile.length() / this.blockSize + 1) * this.blockSize);
			}

			BlockGroup blockGroup = BlockGroup.allocate(content, this.blockSize);
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
				this.openReadingFile();
			}

			blockGroup = BlockGroup.read(this.readingFile, this.blockSize);
			
			/**
			 *읽은 데이터(blockGroup)이 파일의 마지막이라면 readingFile을 삭제하거나 백업한다.
			 */
			if((blockGroup != null && ArrayUtils.isEquals(blockGroup.array(), this.DATAFILE_END)) || blockGroup == null) 
			{
				if(this.readingFileNo.longValue() < writingFileNo.longValue()) 
				{
					if(this.readingFile != null) 
					{
						this.readingFile.close();
						this.readingFile = null;
						
						//readingFile을 사용 완료 후 백업을 원한다면 
						if(this.bakReadFile) 
						{
							try 
							{
								String fileName = this.getDataFileName(this.readingFileNo.longValue());
								FileUtils.moveFileToDirectory(new File(baseDir, fileName), backDir, true);
							} 
							catch(IOException e) 
							{
								String fileName = this.getDataFileName(this.readingFileNo.longValue());
								log.warn("Move file({}) to dir({}) fail.", new File(baseDir, fileName), backDir);
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
					
					this.openReadingFile();
					
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

	public long readingFileOffset() 
	{
		return this.readingOffset.longValue();
	}

	public long readingFileNo() 
	{
		return this.readingFileNo.longValue();
	}

	public void close() 
	{
		if(readingFile != null) 
		{
			try
			{
				this.readingFile.close();
			} 
			catch(IOException e) 
			{
				long fileNo = this.readingFileNo.longValue();
				log.warn("Close reading file({}) fail.", this.getDataFileName(fileNo));
			}
		}

		if (writingFile != null) 
		{
			try
			{
				this.writingFile.close();
			} 
			catch(IOException e) 
			{
				long fileNo = this.writingFileNo.longValue();
				log.warn("Close reading file({}) fail.", this.getDataFileName(fileNo));
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.geekhua.filequeue.datastore.DataStore#writingFileNo()
	 */
	public long writingFileNo() 
	{
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
