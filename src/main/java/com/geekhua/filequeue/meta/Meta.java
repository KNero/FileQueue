package com.geekhua.filequeue.meta;

/**
 * 
 * @author Leo Liang
 * 
 */
class Meta {
    private long readingFileNo;
    private long readingFileOffset;

    Meta(long readingFileNo, long readingFileOffset) {
        super();
        this.readingFileNo = readingFileNo;
        this.readingFileOffset = readingFileOffset;
    }

    long getReadingFileNo() {
        return readingFileNo;
    }

    long getReadingFileOffset() {
        return readingFileOffset;
    }
}
