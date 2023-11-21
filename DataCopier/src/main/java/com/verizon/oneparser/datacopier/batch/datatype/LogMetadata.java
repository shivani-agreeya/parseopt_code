package com.verizon.oneparser.datacopier.batch.datatype;

import com.verizon.oneparser.datacopier.persistence.entity.Log;
import lombok.Data;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

@Data
public class LogMetadata {
    private Log log;
    private File file;
    private Integer userId;
    private final String createdDate;

    public LogMetadata(Log log, File file, Integer userId) {
        this.log = log;
        this.file = file;
        this.userId = userId;
        this.createdDate = new SimpleDateFormat("MMddyyyy").format(new Date());
    }
}
