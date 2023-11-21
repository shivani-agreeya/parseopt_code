package com.verizon.oneparser.writesequencetohdfs.processor

import com.verizon.oneparser.avroschemas.Logs
import com.verizon.oneparser.writesequencetohdfs.Config
import com.verizon.oneparser.writesequencetohdfs.DirType
import com.verizon.oneparser.writesequencetohdfs.getLogger
import com.verizon.oneparser.writesequencetohdfs.isNumeric
import java.text.SimpleDateFormat
import java.util.*
import javax.inject.Singleton

@Singleton
class LogsProcessor(private val hiveProcessor: HiveProcessor,
                    private val config: Config) {
    private val logger = getLogger(javaClass)

    fun process(log: Logs) {
        val sourcePath = sourceFileFullPath(log.getFileName(), log.getFileLocation(), log.getUpdatedTime())
        logger.info("Current Sequencing File : $sourcePath")
        val updatedDate = getDirDateFromPath(sourcePath)
        val outputPath = getHDFSDirPath(DirType.SEQ, "", updatedDate, config.hdfs.fileDirPath)
        hiveProcessor.writeSequenceFile(log, sourcePath, outputPath)
        logger.info("Finished WriteSequenceToHDFS process ==>>")
    }

    private fun sourceFileFullPath(fileName: CharSequence, fileLocation: CharSequence, updatedTime: Long): String {
        val time = millisToString(updatedTime)
        val createdDate: String = getDateFromFileLocation(fileLocation.toString())
        return getHDFSDirPath(DirType.DLF, createdDate, time, config.hdfs.fileDirPath) + fileName
    }

    private fun millisToString(milliSeconds: Long): String {
        val formatter = SimpleDateFormat("MMddyyyy")
        val calendar = Calendar.getInstance()
        calendar.timeInMillis = milliSeconds
        return formatter.format(calendar.time)
    }

    private fun getDateFromFileLocation(fileLocation: String): String {
        var date = ""
        if (fileLocation.isNotEmpty()) {
            val fileLocSplit = fileLocation.split("/")
            if (fileLocSplit.isNotEmpty()) {
                val len = fileLocSplit.size
                var position = len - 1
                date = fileLocSplit[position]
                if (!date.isNumeric()) {
                    position = len - 2
                    date = fileLocSplit[position]
                }
            }
        }
        return date
    }

    private fun getHDFSDirPath(dirType: DirType, createdDate: String, updatedDate: String, dir: String): String {
        val dirPath = if (dir.endsWith('/')) dir else "$dir/"
        return if (createdDate.isNotEmpty()) {
            if (createdDate != updatedDate) when (dirType) {
                DirType.DLF -> "$dirPath$createdDate/processed/"
                DirType.SEQ -> "$dirPath$createdDate/sequence/"
            } else when (dirType) {
                DirType.DLF -> "$dirPath$updatedDate/processed/"
                DirType.SEQ -> "$dirPath$updatedDate/sequence/"
            }
        } else if (updatedDate != fetchUpdatedDirDate()) when (dirType) {
            DirType.DLF -> "$dirPath$updatedDate/processed/"
            DirType.SEQ -> "$dirPath$updatedDate/sequence/"
        } else when (dirType) {
            DirType.DLF -> dirPath + fetchUpdatedDirDate() + "/processed/"
            DirType.SEQ -> dirPath + fetchUpdatedDirDate() + "/sequence/"
        }
    }

    private fun getDirDateFromPath(filePath: String): String {
        val dateFormatter = SimpleDateFormat("MMddyyyy")
        val pathSplit = filePath.split("/processed")
        var date = dateFormatter.format(Date())
        if (pathSplit.isNotEmpty()) {
            val fileLocSplit = pathSplit[0].split("/")
            if (fileLocSplit.isNotEmpty()) {
                val len = fileLocSplit.size
                date = fileLocSplit[len - 1]
            }
        }
        return date
    }

    private fun fetchUpdatedDirDate(): String {
        val dateFormatter = SimpleDateFormat("MMddyyyy")
        return dateFormatter.format(Date())
    }
}
