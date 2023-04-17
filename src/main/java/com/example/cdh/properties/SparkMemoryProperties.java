package com.example.cdh.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author chunyang.leng
 * @date 2023-04-12 13:58
 */
@ConfigurationProperties("spark.memory")
@Configuration
public class SparkMemoryProperties {
    /**
     * 执行内存和缓存内存（堆）占jvm总内存的比例，剩余的部分是spark留给用户存储内部源数据、数据结构、异常大的结果数据。
     * 默认值0.6，调小会导致频繁gc，调大容易造成oom。
     */
    private Float fraction;

    /**
     * 用于存储的内存在堆中的占比，默认0.5。调大会导致执行内存过小，执行数据落盘，影响效率；调小会导致缓存内存不够，缓存到磁盘上去，影响效率。
     *
     * 值得一提的是在spark中，执行内存和缓存内存公用java堆，当执行内存没有使用时，会动态分配给缓存内存使用，反之也是这样。如果执行内存不够用，可以将存储内存释放移动到磁盘上（最多释放不能超过本参数划分的比例），但存储内存不能把执行内存抢走。
     */
    private Float storageFraction;

    private OffHeap offHeap;

    public Float getFraction() {
        return fraction;
    }

    public void setFraction(Float fraction) {
        this.fraction = fraction;
    }

    public Float getStorageFraction() {
        return storageFraction;
    }

    public void setStorageFraction(Float storageFraction) {
        this.storageFraction = storageFraction;
    }

    public OffHeap getOffHeap() {
        return offHeap;
    }

    public void setOffHeap(OffHeap offHeap) {
        this.offHeap = offHeap;
    }

    static class OffHeap{
        /**
         * 是否允许使用堆外内存来进行某些操作。默认false
         */
        private Boolean enabled;
        /**
         * 允许使用进行操作的堆外内存的大小，单位bytes 默认0
         */
        private Long size;

        public Boolean getEnabled() {
            return enabled;
        }

        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }

        public Long getSize() {
            return size;
        }

        public void setSize(Long size) {
            this.size = size;
        }
    }
}
