package com.verizon.oneparser.hdfscopier.job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@EnableScheduling
@RequiredArgsConstructor
public class JobScheduler {
    private final HdfsCopyJob hdfsCopyJob;

    @Scheduled(fixedRateString = "${hdfs.job.fixedRate:60000}")
    public void scheduleHdfsCopy() {
        hdfsCopyJob.execute();
    }
}
