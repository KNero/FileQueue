package com.geekhua.filequeue.datastore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import com.geekhua.filequeue.Config;
import com.geekhua.filequeue.codec.Codec;
import com.geekhua.filequeue.codec.CodecFactory;

/**
 * 
 * @author Leo Liang
 * 
 */
public class DataStoreImpl<E> implements DataStore<E> {
    private static final String DATAFILE_DIRNAME     = "data";
    private static final String DATAFILE_PREFIX      = "fdata-";
    private static final String DATAFILE_SUFIX       = ".fq";

    private static final byte[] DATAFILE_END_CONTENT = new byte[] { (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAA,
            (byte) 0xAA, (byte) 0xAA, (byte) 0xAA, (byte) 0xAB };

    private byte[]              DATAFILE_END;
    private File                baseDir;
    private int                 blockSize;
    private Codec<E>            codec;
    private long                fileSize;
    private String              name;
    private RandomAccessFile    readingFile          = null;
    private AtomicLong          readingFileNo        = new AtomicLong(-1L);
    private long                readingOffset        = 0L;

    private AtomicLong          writingFileNo        = new AtomicLong(-1L);
    private RandomAccessFile    writingFile          = null;

    public DataStoreImpl(Config config) {
        this.name = config.getName();
        this.baseDir = new File(new File(config.getBaseDir(), name), DATAFILE_DIRNAME);
        this.blockSize = BlockGroup.estimateBlockSize(config.getMsgAvgLen());
        this.readingFileNo = new AtomicLong(config.getReadingFileNo());
        this.readingOffset = config.getReadingOffset();
        this.codec = CodecFactory.getInstance(config.getCodec());
        this.fileSize = config.getFileSize();
    }

    private boolean createBaseDirIfNeeded() {
        if (!baseDir.exists()) {
            return baseDir.mkdirs();
        }
        return true;
    }

    private void createNewWriteFile() {
        if (writingFile != null) {
            try {
                writingFile.write(DATAFILE_END);
                writingFile.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            writingFile = null;
        }

        String newWriteFileName = getNewWriteFileName();
        if (StringUtils.isNotBlank(newWriteFileName)) {
            try {
                writingFile = new RandomAccessFile(new File(baseDir, newWriteFileName), "rw");
            } catch (FileNotFoundException e) {
                throw new IllegalStateException(String.format("File(%s) not found", newWriteFileName));
            }
        } else {
            throw new IllegalStateException("Can not create new write file");
        }
    }

    private long getFileNumber(String fileName) {
        return StringUtils.isBlank(fileName) ? 0L : Long.valueOf(fileName.substring(DATAFILE_PREFIX.length(),
                fileName.length() - DATAFILE_SUFIX.length()));
    }

    public String getNewWriteFileName() {
        return getDataFileName(writingFileNo.incrementAndGet());
    }

    public String getDataFileName(long fileNo) {
        return DATAFILE_PREFIX + String.format("%018d", fileNo) + DATAFILE_SUFIX;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.geekhua.filequeue.datastore.DataStore#init()
     */
    public void init() throws IOException {
        BlockGroup endFileBlockGroup = BlockGroup.allocate(DATAFILE_END_CONTENT.length, blockSize);
        endFileBlockGroup.setContent(DATAFILE_END_CONTENT);
        this.DATAFILE_END = endFileBlockGroup.array();

        createBaseDirIfNeeded();
        getLastDataFileNo();
        recoverLastDataFileIfNeeded();
        createNewWriteFile();
        openReadingFile();
    }

    private void recoverLastDataFileIfNeeded() throws IOException {
        if (writingFileNo.longValue() >= 0) {
            RandomAccessFile lastWriteFile = null;
            try {
                lastWriteFile = new RandomAccessFile(new File(baseDir, getDataFileName(writingFileNo.longValue())),
                        "rw");
                if (lastWriteFile.length() % blockSize != 0) {
                    long newLength = (lastWriteFile.length() / blockSize + 1) * blockSize;
                    lastWriteFile.seek(newLength);
                    lastWriteFile.setLength(newLength);
                }

                if (lastWriteFile.length() >= DATAFILE_END.length) {
                    lastWriteFile.seek(lastWriteFile.length() - DATAFILE_END.length);
                }
                BlockGroup end = BlockGroup.read(lastWriteFile, blockSize);
                if (!ArrayUtils.isEquals(end, DATAFILE_END)) {
                    lastWriteFile.write(DATAFILE_END);
                }
            } finally {
                if (lastWriteFile != null) {
                    lastWriteFile.close();
                }
            }

        }
    }

    private void getLastDataFileNo() {
        long maxDataFileNo = -1L;
        String[] dataFilesArr = baseDir.list(new FilenameFilter() {

            public boolean accept(File dir, String name) {
                if (StringUtils.endsWith(name, DATAFILE_SUFIX) && StringUtils.startsWith(name, DATAFILE_PREFIX)) {
                    return true;
                }

                return false;
            }
        });

        for (String dataFile : dataFilesArr) {
            long fileNo = getFileNumber(dataFile);
            if (fileNo > maxDataFileNo) {
                maxDataFileNo = fileNo;
            }
        }

        writingFileNo.set(maxDataFileNo);
    }

    private void openReadingFile() {
        if (readingFileNo.longValue() < 0 && writingFileNo.longValue() >= 0) {
            readingFileNo = new AtomicLong(writingFileNo.longValue());
            readingOffset = 0L;
        }

        if (readingFileNo.longValue() >= 0) {
            try {
                readingFile = new RandomAccessFile(new File(baseDir, getDataFileName(readingFileNo.longValue())), "r");
                readingOffset = 0L;

                if (readingOffset > 0L) {
                    readingFile.seek(readingOffset % blockSize == 0 ? readingOffset
                            : ((readingOffset / blockSize + 1) * blockSize));
                }
            } catch (IOException e) {
                throw new IllegalStateException(String.format("File(%s) open fail",
                        getDataFileName(readingFileNo.longValue())));
            }
        }
    }

    public void put(E element) throws IOException {
        byte[] content = codec.encode(element);
        if (content != null && content.length != 0) {
            if (writingFile.length() >= fileSize) {
                createNewWriteFile();
            }

            BlockGroup blockGroup = BlockGroup.allocate(content.length, blockSize);
            blockGroup.setContent(content);

            if (writingFile.length() % blockSize != 0) {
                writingFile.seek((writingFile.length() / blockSize + 1) * blockSize);
            }
            writingFile.write(blockGroup.array());
        }
    }

    public E take() throws IOException {
        BlockGroup blockGroup = null;

        if (readingFileNo.longValue() >= 0) {
            blockGroup = BlockGroup.read(readingFile, blockSize);
            if (blockGroup != null && ArrayUtils.isEquals(blockGroup.array(), DATAFILE_END)) {
                if (readingFileNo.longValue() < writingFileNo.longValue()) {
                    if (readingFile != null) {
                        readingFile.close();
                    }
                    readingFileNo.incrementAndGet();
                    openReadingFile();
                    return take();
                }
            }
        }
        if (blockGroup == null) {
            return null;
        } else {
            readingOffset = readingFile.getFilePointer();
            return codec.decode(blockGroup.getContent());
        }

    }

    public long readingFileOffset() {
        return readingOffset;
    }

    public long readingFileNo() {
        return readingFileNo.longValue();
    }

}