package com.zhaopin.tendis.service;

import com.zhaopin.tendis.common.RecordKey;
import com.zhaopin.tendis.common.RecordType;
import org.apache.commons.lang3.tuple.Pair;
import org.bouncycastle.util.Pack;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class TendisAnalysisService {

    public static Logger LOGGER = LoggerFactory.getLogger(TendisAnalysisService.class);

    final static int CHUNKID_OFFSET = 0;

    final static int TYPE_OFFSET = CHUNKID_OFFSET + Integer.BYTES;

    final static int DBID_OFFSET = TYPE_OFFSET + Byte.BYTES;

    final static int PK_OFFSET = DBID_OFFSET + Integer.BYTES;

    final static int UINT8_MAX = 0xFF;

    public TendisAnalysisService() {

    }

    public void execute(final String dbPath) throws Exception {
        long sucessCount = 0, unknownCount = 0;
        Options option = new Options();
        Filter filter = new BloomFilter(10);
        option.setCreateIfMissing(false);
        RocksDB rocksDB = RocksDB.openReadOnly(option, dbPath);
        try {
            RocksIterator it = rocksDB.newIterator();
            for (it.seekToFirst(); it.isValid(); it.next()) {
                byte[] key = it.key();
                byte[] value = it.value();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("key={}, value={}", key, value);
                }
                final RecordKey rk = decode(key);
                if (rk != null) {
                    ++sucessCount;
                } else {
                    ++unknownCount;
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("decode key result is {}", rk);
                }
                if ((sucessCount + unknownCount) % 1000000 == 0) {
                    LOGGER.info("the analysis intermediate result is success count={}, unknown count={}", sucessCount, unknownCount);
                }
            }
            LOGGER.info("the analysis final result is success count={}, unknown count={}", sucessCount, unknownCount);
        } finally {
            rocksDB.close();
        }
    }

    public static RecordKey decode(byte[] bytes) {
        int rsvd = Byte.BYTES;
        int offset = PK_OFFSET;
        int chunkId = Pack.bigEndianToInt(bytes, 0);
        byte type = bytes[TYPE_OFFSET];
        RecordType rt = char2Rt(type);
        if (rt == RecordType.RT_INVALID) {
            return null;
        }
        int dbid = Pack.bigEndianToInt(bytes, DBID_OFFSET);
        int poffset = bytes.length - rsvd - 1;
        int maxSize = bytes.length - rsvd - offset;
        final Optional<Pair<Long, Integer>> pkResult = varintDecodeRvs(bytes, poffset, maxSize);
        if (!pkResult.isPresent()) {
            return null;
        }
        int rvsOffset = 0;
        long pkLen = 0;
        rvsOffset += pkResult.get().getRight();
        pkLen = pkResult.get().getLeft();
        String pk = new String(bytes, offset, (int) pkLen);
        int left = bytes.length - offset - rsvd - rvsOffset - (int) pkLen - 1;
        final Optional<Pair<Long, Integer>> versionResult = varintDecodeFwd(bytes, offset + (int) pkLen + 1, left);
        if (!versionResult.isPresent()) {
            return null;
        }
        int versionLen = versionResult.get().getRight();
        long version = versionResult.get().getLeft();
        int skLen = left - versionLen;
        String sk = "";
        if (skLen > 0) {
            sk = new String(bytes, offset + (int) pkLen + 1 + versionLen, (int) skLen);
        }
        return RecordKey.builder().chunkId(chunkId).type(rt).sk(sk).pk(pk).version(version).build();
    }

    public static Optional<Pair<Long, Integer>> varintDecodeRvs(byte[] bytes, final int offset, final int maxSize) {
        long ret = 0;
        int i = 0;
        while (i < maxSize && (bytes[offset - i] & 0x80) != 0) {
            ret |= (long) (bytes[offset - i] & 0x7F) << (7 * i);
            i++;
        }
        if (i == maxSize) {
            return Optional.empty();
        }
        ret |= (long) (bytes[offset - i] & 0x7F) << (7 * i);
        ++i;
        return Optional.of(Pair.of(ret, i));
    }

    public static Optional<Pair<Long, Integer>> varintDecodeFwd(byte[] bytes, final int offset, final int maxSize) {
        long ret = 0;
        int i = 0;
        while (i < maxSize && (bytes[offset + i] & 0x80) != 0) {
            ret |= (bytes[offset + i] & 0x7F) << (7 * i);
            ++i;
        }
        if (i == maxSize) {
            return Optional.empty();
        }
        ret |= (bytes[offset + i] & 0x7F) << (7 * i);
        ++i;
        return Optional.of(Pair.of(ret, i));
    }

    public static RecordType char2Rt(byte t) {
        switch (t) {
            case 'M':
                return RecordType.RT_META;
            case 'D':
                return RecordType.RT_DATA_META;
            case 'a':
                return RecordType.RT_KV;
            case 'L':
                return RecordType.RT_LIST_META;
            case 'l':
                return RecordType.RT_LIST_ELE;
            case 'H':
                return RecordType.RT_HASH_META;
            case 'h':
                return RecordType.RT_HASH_ELE;
            case 'S':
                return RecordType.RT_SET_META;
            case 's':
                return RecordType.RT_SET_ELE;
            case 'Z':
                return RecordType.RT_ZSET_META;
            case 'z':
                return RecordType.RT_ZSET_S_ELE;
            case 'c':
                return RecordType.RT_ZSET_H_ELE;
            default:
                return RecordType.RT_INVALID;
        }
    }

}
