package com.zhaopin.tendis.common;

import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class RecordKey {

    private int chunkId;
    private int dbId;
    private RecordType type;
    private String pk;
    private String sk;
    // version for subkey, it would be always 0 for *_META.
    private long version;

}
