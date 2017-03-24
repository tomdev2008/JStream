package com.sdu.jstorm.data;

import com.sdu.jstorm.data.internal.JDataSource;
import com.sdu.jstorm.translator.JDataTranslator;
import lombok.Getter;
import lombok.Setter;

/**
 * @author hanhan.zhang
 * */
public class JDataInputConfig<T> {

    @Setter
    private JDataTranslator<T> translator;
    @Setter
    private JDataSource<T> dataSource;
    @Setter
    private boolean autoCommit;
    @Setter
    @Getter
    private long autoCommitPeriodMs;
    @Setter
    @Getter
    private long maxRetryTimes;

    public JDataTranslator<T> getDataTranslator() {
        return translator;
    }

    public JDataSource<T> getDataSource() {
        return dataSource;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }
}
