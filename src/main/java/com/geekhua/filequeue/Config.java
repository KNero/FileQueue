package com.geekhua.filequeue;

import com.geekhua.filequeue.codec.Codec;
import com.geekhua.filequeue.codec.ObjectCodec;

/**
 * 
 * @author Leo Liang
 */
public class Config {

    private Codec   codec         = new ObjectCodec();
    private String  name          = "default";
    private String  baseDir       = "/data/appdatas/filequeue";
    private int     msgAvgLen     = 1024;
    private long    readingFileNo = -1L;
    private long    readingOffset = 0L;
    private long    fileSiz       = 1024 * 1024 * 100;
    private boolean bakReadFile   = false;

    public boolean isBakReadFile() {
        return bakReadFile;
    }

    public void setBakReadFile(boolean bakReadFile) {
        this.bakReadFile = bakReadFile;
    }

    public long getFileSiz() {
        return fileSiz;
    }

    public void setFileSiz(long fileSiz) {
        this.fileSiz = fileSiz;
    }

    public void setCodec(Codec codec) {
        this.codec = codec;
    }

    /**
     * 큐 이름을 지정한다.
     * default name : default
     * @param name 큐 폴더 이름
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * 큐 폴더가 생성될 위치를 지정한다.<br>
     * BaseDir 밑에 큐 폴더가 생성된다.
     * @param baseDir 큐가 저장될 폴더 위치
     */
    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

    /**
     * 저장될 메시지의 평균 길이를 입력한다.
     * 평균길이를 활용하여 BlockGroup에서 사용될 버퍼의 크기를 정한다.
     * @param _msgAvgLen
     */
    public void setMsgAvgLen(int _msgAvgLen) 
    {
    	if(_msgAvgLen <= 0)
    	{
    		throw new IllegalArgumentException("_msgAvgLen is bigger than zero.(_msgAvgLen > 0)");
    	}
    	
        this.msgAvgLen = _msgAvgLen;
    }

    public void setReadingFileNo(long readingFileNo) {
        this.readingFileNo = readingFileNo;
    }

    public void setReadingOffset(long readingOffset) {
        this.readingOffset = readingOffset;
    }

    public Codec getCodec() {
        return codec;
    }

    public String getName() {
        return name;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public int getMsgAvgLen() {
        return msgAvgLen;
    }

    public long getReadingFileNo() {
        return readingFileNo;
    }

    public long getReadingOffset() {
        return readingOffset;
    }

    public long getFileSize() {
        return fileSiz;
    }

}
