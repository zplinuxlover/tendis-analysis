package com.zhaopin.tendis.common;

public enum RecordType {
    RT_INVALID,
    RT_META,       /* For catalog */
    RT_KV,         /* For realtype in RecordValue */
    RT_LIST_META,  /* For realtype in RecordValue */
    RT_LIST_ELE,   /* For list subkey type in RecordKey and RecordValue */
    RT_HASH_META,  /* For realtype in RecordValue */
    RT_HASH_ELE,   /* For hash subkey type in RecordKey and RecordValue  */
    RT_SET_META,   /* For realtype in RecordValue */
    RT_SET_ELE,    /* For set subkey type in RecordKey and RecordValue  */
    RT_ZSET_META,  /* For realtype in RecordValue */
    RT_ZSET_S_ELE, /* For zset subkey type in RecordKey and RecordValue  */
    RT_ZSET_H_ELE, /* For zset subkey type in RecordKey and RecordValue  */
    RT_BINLOG,     /* For binlog in RecordKey and RecordValue  */
    RT_TTL_INDEX,  /* For ttl index  in RecordKey and RecordValue  */
    RT_DATA_META,  /* For key type in RecordKey */
}
