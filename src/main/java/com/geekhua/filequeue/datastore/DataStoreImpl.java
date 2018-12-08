package com.geekhua.filequeue.datastore;

import com.geekhua.filequeue.Config;
import com.geekhua.filequeue.codec.Codec;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicLong;

public class DataStoreImpl<E> implements DataStore<E> {
    private static final Logger log = LoggerFactory.getLogger(DataStoreImpl.class);

	private static final String DATAFILE_DIRNAME = "data";
	private static final String DATAFILE_PREFIX = "q-";
	private static final String DATAFILE_EXTENSION = ".fq";
	private static final String DATAFILE_BACK_DIR = "bak";

	private final byte[] endBlock;

	private File baseDir;
	private File backDir;
	private int blockSize;
	private Codec codec;
	private long maxFileSize;
	
	private RandomAccessFile readingFile = null;
	private AtomicLong readingFileNo;
	private AtomicLong readingOffset;
	private boolean isBackupReadFile;

	private AtomicLong writingFileNo = new AtomicLong(-1L);
	private RandomAccessFile writingFile = null;

	public DataStoreImpl(Config config, long readingFileNo, long readingOffset) {
		String name = config.getName();
		this.baseDir = new File(new File(config.getBaseDir(), name), DATAFILE_DIRNAME);

		final byte[] endContent = new byte[] {(byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAB};
		this.blockSize = BlockGroup.estimateBlockGroupSize(config.getMsgAvgLen());
		endBlock = BlockGroup.allocate(endContent, blockSize).array();

		this.readingFileNo = new AtomicLong(readingFileNo);
		this.readingOffset = new AtomicLong(readingOffset);
		this.codec = config.getCodec();
		this.maxFileSize = config.getFileSize();
		this.isBackupReadFile = config.isBackupReadFile();
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
		findLastWroteFileNo();
		closeLastWroteFile();
		createNewWriteFile();
		
		checkReadingFile();
		openReadingFile();
	}

	private void createBaseDirIfNeeded() throws IOException {
		if (!baseDir.exists() && !baseDir.mkdirs()) {
            throw new IOException("Can not create queue data directory. " + baseDir.getAbsolutePath());
		}
	}

	private void createBakDirIfNeeded() throws IOException {
		if (!backDir.exists() && !backDir.mkdirs()) {
			throw new IOException("Can not create queue backup directory. {}" + backDir.getAbsolutePath());
		}
	}

    /**
     * 큐파일 중에서 가장 마지막에 생성된 파일을 찾는다.
     */
    private void findLastWroteFileNo() {
        String[] dataFilesArr = baseDir.list(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return StringUtils.endsWith(name, DATAFILE_EXTENSION) && StringUtils.startsWith(name, DATAFILE_PREFIX);
            }
        });

        // 전에 사용한 큐파일이 없을 경우 -1 로 세팅한다.
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
    private void closeLastWroteFile() throws IOException {
        if (writingFileNo.longValue() >= 0) {
            String fileName = getDataFileName(writingFileNo.longValue());

            File lastFile = new File(baseDir, fileName);
            if (lastFile.length() % blockSize != 0) {
                throw new IOException("file size % block size != 0. file size:" + lastFile.length() + ", block size:" + blockSize);
            }

            try (FileOutputStream lastWriteFile = new FileOutputStream(lastFile, true)) {
                lastWriteFile.write(endBlock);
            }
        }
    }

	private void createNewWriteFile() throws IOException {
        // 실행 단계에서는 null 이기 때문에 실행되지 않는다.
		if(this.writingFile != null) {
			//파일이 끝났다는 것을 표시한다.
			this.writingFile.write(endBlock);
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
		if(this.readingFileNo.longValue() >= 0) {
			String fileName = getDataFileName(this.readingFileNo.longValue());

			try {
				this.readingFile = new RandomAccessFile(new File(baseDir, fileName), "r");

				long readPosition = readingOffset.longValue();
				if(readPosition > 0L) {
					readingFile.seek(readPosition);
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

				throw new IllegalStateException(String.format("File(%s) open fail",	fileName), e);
			}
		}
	}

	public void put(E element) throws IOException {
		byte[] content = codec.encode(element);

		if(content != null && content.length > 0) {
			long writingFileSize = writingFile.length();
			if (writingFileSize % blockSize != 0) {
				writingFile.seek((writingFileSize / blockSize + 1) * blockSize);
				writingFile.write(endBlock);
				log.error("Fail to write file. So new file created. file size % block size != 0. file size:{}, block size:{}", writingFileSize, blockSize);

				createNewWriteFile();
			} else if(this.writingFile.length() >= this.maxFileSize) {
				createNewWriteFile();
			}

			BlockGroup blockGroup = BlockGroup.allocate(content, this.blockSize);
			this.writingFile.write(blockGroup.array());
		}
	}

	@SuppressWarnings("unchecked")
	public E take() throws IOException {
		BlockGroup blockGroup = null;

		if(this.readingFileNo.longValue() >= 0) {
			blockGroup = BlockGroup.read(this.readingFile, this.blockSize);
			
			// 읽은 데이터(blockGroup)이 파일의 마지막이라면 readingFile 을 삭제하거나 백업한다.
			boolean isEndFile = blockGroup != null && ArrayUtils.isEquals(blockGroup.array(), endBlock);
			if(isEndFile || blockGroup == null) {
				return completeReadingFile();
			}
		}
		
		if(blockGroup == null) {
			return null;
		} else {
			this.readingOffset.set(this.readingFile.getFilePointer());
			return (E)codec.decode(blockGroup.getContent());
		}
	}

	/**
	 * 읽기가 완료된 파일일 경우 백업이 필요하면 백업을 수행하고 새로운 파일을 열고 읽기를 시작한다.
	 */
	private E completeReadingFile() throws IOException {
		if(this.readingFileNo.longValue() < writingFileNo.longValue()) {
			this.readingFile.close();
			this.readingFile = null;

			if(this.isBackupReadFile) {
				try {
					String fileName = getDataFileName(this.readingFileNo.longValue());
					FileUtils.moveFileToDirectory(new File(baseDir, fileName), backDir, true);
				} catch (IOException e) {
					String fileName = getDataFileName(this.readingFileNo.longValue());
					log.warn("Move file({}) to dir({}) fail.", new File(baseDir, fileName), backDir);
				}
			} else {
				String fileName = getDataFileName(this.readingFileNo.longValue());
				FileUtils.deleteQuietly(new File(baseDir, fileName));
			}

			this.readingFileNo.incrementAndGet();
			this.readingOffset.set(0L);

			this.openReadingFile();

			return take();
		}

		return null;
	}

	public long readingFileOffset() {
		return this.readingOffset.longValue();
	}

	public long readingFileNo() {
		return this.readingFileNo.longValue();
	}

	public long writingFileNo()
	{
		return writingFileNo.get();
	}

	public long writingFileOffset() {
		try {
			return writingFile.getFilePointer();
		} catch (IOException e) {
			return -1;
		}
	}

	public void close() {
		if (readingFile != null) {
			try {
				this.readingFile.close();
			} catch (IOException e) {
				log.warn("Close reading file({}) fail.", getDataFileName(readingFileNo.longValue()));
			}
		}

		if (writingFile != null) {
			try {
				this.writingFile.close();
			} catch (IOException e) {
				log.warn("Close reading file({}) fail.", getDataFileName(writingFileNo.longValue()));
			}
		}
	}
}
