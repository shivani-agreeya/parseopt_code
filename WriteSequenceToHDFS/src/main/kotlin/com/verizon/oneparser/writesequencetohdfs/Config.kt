package com.verizon.oneparser.writesequencetohdfs

import io.micronaut.context.annotation.ConfigurationBuilder
import io.micronaut.context.annotation.ConfigurationProperties

@ConfigurationProperties("app.config")
class Config {
    @ConfigurationBuilder(configurationPrefix = "hdfs")
    var hdfs: Hdfs = Hdfs()
    @ConfigurationBuilder(configurationPrefix = "topics")
    var topics: Topics = Topics()

    class Hdfs {
        var uri: String = "Not set"
        var fileDirPath: String = "Not Set"
    }

    class Topics {
        var groupId: String = "\${micronaut.application.name}"
        var nservice: String = "nservice"
        var hiveprocessor: String = "hiveprocessor"
        var listen: List<String> = listOf("dlf, dml_dlf")
    }
}

