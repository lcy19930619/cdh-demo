package com.example.cdh.properties.spark;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author chunyang.leng
 * @date 2023-04-12 13:34
 */
@Configuration
@ConfigurationProperties(prefix = "spark.shuffle")
public class SparkShuffleProperties {
    /**
     * 是否压缩map输出文件，默认压缩 true
     */
    private String compress;

    /**
     * 该参数代表了Executor内存中，分配给shuffle read task进行聚合操作的内存比例，默认是20%。
     * cache少且内存充足时，可以调大该参数，给shuffle read的聚合操作更多内存，以避免由于内存不足导致聚合过程中频繁读写磁盘。
     */
    private String memoryFraction;
    /**
     * 当ShuffleManager为SortShuffleManager时，如果shuffle read task的数量小于这个阈值（默认是200），则shuffle write过程中不会进行排序操作，而是直接按照未经优化的HashShuffleManager的方式去写数据，但是最后会将每个task产生的所有临时磁盘文件都合并成一个文件，并会创建单独的索引文件。
     * <p>
     * 当使用SortShuffleManager时，如果的确不需要排序操作，那么建议将这个参数调大一些，大于shuffle read task的数量。那么此时就会自动启用bypass机制，map-side就不会进行排序了，减少了排序的性能开销。但是这种方式下，依然会产生大量的磁盘文件，因此shuffle write性能有待提高。
     */
    private String manager;

    /**
     * 如果使用HashShuffleManager，该参数有效。如果设置为true，那么就会开启consolidate机制，会大幅度合并shuffle write的输出文件，对于shuffle read task数量特别多的情况下，这种方法可以极大地减少磁盘IO开销，提升性能。
     * <p>
     * 如果的确不需要SortShuffleManager的排序机制，那么除了使用bypass机制，还可以尝试将spark.shuffle.manager参数手动指定为hash，使用HashShuffleManager，同时开启consolidate机制。
     */
    private String consolidateFiles;

    private Spill spill;

    private File file;

    private Io io;

    public String getCompress() {
        return compress;
    }

    public void setCompress(String compress) {
        this.compress = compress;
    }

    public String getMemoryFraction() {
        return memoryFraction;
    }

    public void setMemoryFraction(String memoryFraction) {
        this.memoryFraction = memoryFraction;
    }

    public String getManager() {
        return manager;
    }

    public void setManager(String manager) {
        this.manager = manager;
    }

    public String getConsolidateFiles() {
        return consolidateFiles;
    }

    public void setConsolidateFiles(String consolidateFiles) {
        this.consolidateFiles = consolidateFiles;
    }

    public Spill getSpill() {
        return spill;
    }

    public void setSpill(Spill spill) {
        this.spill = spill;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public Io getIo() {
        return io;
    }

    public void setIo(Io io) {
        this.io = io;
    }

    static class Spill {
        /**
         * shuffle过程中溢出的文件是否压缩，默认true，使用spark.io.compression.codec压缩。
         */
        private String compress;

        public String getCompress() {
            return compress;
        }

        public void setCompress(String compress) {
            this.compress = compress;
        }
    }

    static class File {
        /**
         * 在内存输出流中 每个shuffle文件占用内存大小，适当提高 可以减少磁盘读写 io次数，初始值为32k
         */
        private String buffer;

        public String getBuffer() {
            return buffer;
        }

        public void setBuffer(String buffer) {
            this.buffer = buffer;
        }
    }

    static class Io {
        /**
         * shuffle read task从shuffle write task所在节点拉取属于自己的数据时，如果因为网络异常导致拉取失败，是会自动进行重试的。该参数就代表了可以重试的最大次数。如果在指定次数之内拉取还是没有成功，就可能会导致作业执行失败。
         * <p>
         * 对于那些包含了特别耗时的shuffle操作的作业，建议增加重试最大次数（比如60次），以避免由于JVM的full gc或者网络不稳定等因素导致的数据拉取失败。在实践中发现，对于针对超大数据量（数十亿~上百亿）的shuffle过程，调节该参数可以大幅度提升稳定性。
         */
        private String maxRetries;
        /**
         * 默认5s，建议加大间隔时长（比如60s），以增加shuffle操作的稳定性
         */
        private String retryWait;

        public String getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(String maxRetries) {
            this.maxRetries = maxRetries;
        }

        public String getRetryWait() {
            return retryWait;
        }

        public void setRetryWait(String retryWait) {
            this.retryWait = retryWait;
        }
    }
}
