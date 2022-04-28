package com.zhaopin.tendis;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.zhaopin.tendis.service.TendisAnalysisService;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TendisAnalysis {

    public static Logger LOGGER = LoggerFactory.getLogger(TendisAnalysis.class);

    public static void main(String[] args) throws Exception {
        final Args analysisArgs = new Args();
        JCommander jcommander = new JCommander(analysisArgs);
        jcommander.setProgramName("TendisAnalysis");
        jcommander.parse(args);
        Preconditions.checkArgument(StringUtils.isNotEmpty(analysisArgs.getRocksDbPath()));
        new TendisAnalysisService().execute(analysisArgs.getRocksDbPath());
    }

    @Getter
    @Setter
    public static class Args {

        @Parameter(names = {"--dbpath"}, description = "the rocksdb db path")
        private String rocksDbPath;
    }

}
