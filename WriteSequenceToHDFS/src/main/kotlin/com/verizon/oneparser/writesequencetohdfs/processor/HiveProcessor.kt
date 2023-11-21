package com.verizon.oneparser.writesequencetohdfs.processor

import com.verizon.oneparser.avroschemas.Logs
import com.verizon.oneparser.writesequencetohdfs.Config
import com.verizon.oneparser.writesequencetohdfs.Constants
import com.verizon.oneparser.writesequencetohdfs.LogStatus
import com.verizon.oneparser.writesequencetohdfs.Parser
import com.verizon.oneparser.writesequencetohdfs.SequenceObj
import com.verizon.oneparser.writesequencetohdfs.getLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.SequenceFile
import java.util.*
import javax.inject.Singleton
import kotlin.system.measureTimeMillis

@Singleton
class HiveProcessor(private val config: Config) {
    private val logger = getLogger(javaClass)

    fun writeSequenceFile(log: Logs, sourcePath: String, outputPath: String) {
        val broadCastValue = getHDFSBroadcastConfig()
        val fs: FileSystem = FileSystem.get(broadCastValue)
        val filePath = Path(sourcePath)
        if (fs.exists(filePath)) {
            fs.open(filePath).use { dlfData ->
                val time = measureTimeMillis {
                    run(dlfData, log, sourcePath, broadCastValue, outputPath)
                }
                logger.info("Processed: $sourcePath Elapsed time: ${time}ms")
            }
            deleteFile(sourcePath, broadCastValue)
        } else {
            logger.info("File $sourcePath does not exist")
            log.setStatus(LogStatus.DLF_DOES_NOT_EXIST)
        }
    }

    private fun deleteFile(fileName: String, broadCastValue: Configuration) {
        logger.info("deleting $fileName")
        val fs: FileSystem = FileSystem.get(broadCastValue)
        fs.delete(Path(fileName), true)
    }

    private fun run(din: FSDataInputStream, log: Logs, sourcePath: String, broadCastValue: Configuration, outputPath: String) {
        var startLogTime: Date? = null
        var endLogTime: Date? = null
        val seqFileName = getSeqFileName(sourcePath)
        logger.info("START Processing Thread for file: $outputPath$seqFileName")
        val seqObj = SequenceObj()
        SequenceFile.createWriter(
                broadCastValue,
                SequenceFile.Writer.file(Path(outputPath + seqFileName)),
                SequenceFile.Writer.keyClass(IntWritable::class.java),
                SequenceFile.Writer.valueClass(BytesWritable::class.java)
        ).use { writer ->
            logger.info("Current File Name : $seqFileName")
            val isSigFile = seqFileName.contains("_sig.seq")
            while (!seqObj.endOfFile) {
                if (isSigFile) Parser.nextRecordSig(din, writer, seqObj) else Parser.nextRecord(din, writer, seqObj)
                if (seqObj.lcTime != null) {
                    if (startLogTime == null) {
                        startLogTime = seqObj.lcTime
                    }
                    endLogTime = seqObj.lcTime
                }
            }
        }
        logger.info("FINISHED Processing Thread for file : $seqFileName ==>> ")

        log.apply {
            setStatus(LogStatus.SEQ_DONE)
            setStartLogTime(startLogTime?.time)
            setLastLogTime(endLogTime?.time)
            setSeqEndTime(System.currentTimeMillis())
            setFileName(seqFileName)
        }
    }

    private fun getSeqFileName(filePath: String): String {
        val compositeFormat = """(.*)/(.*).(${Constants.CONST_DRM_DLF}|${Constants.CONST_DML_DLF}|${Constants.CONST_SIG_DLF})""".toRegex()
        val sigFormat = """(.*)/(.*).(${Constants.CONST_SIG})""".toRegex()
        val dlfFormat = """(.*)/(.*).(${Constants.CONST_DLF})""".toRegex()
//        filePath match {
//            case compositeFormat(_, fileNameWithoutExtension, extension, _*) => fileNameWithoutExtension + "_" + extension + ".seq"
//            case sigFormat(_, fileNameWithoutExtension, _*) => fileNameWithoutExtension + "_sig.seq"
//            case dlfFormat(_, fileNameWithoutExtension, _*) => fileNameWithoutExtension + ".seq"
//            case _ => throw new Exception("Unsupported log format")
//        }
        return when {
            compositeFormat.matches(filePath) -> {
                val a = compositeFormat.find(filePath)!!.groups.map { it!!.value }
                "${a[2]}_${a[3]}.seq"
            }
            sigFormat.matches(filePath) -> {
                val a = sigFormat.find(filePath)!!.groups.map { it!!.value }
                "${a[2]}_sig.seq"
            }
            dlfFormat.matches(filePath) -> {
                val a = dlfFormat.find(filePath)!!.groups.map { it!!.value }
                "${a[2]}.seq"
            }
            else -> ""
        }
    }

    private fun getHDFSBroadcastConfig(): Configuration {
        val broadcastConfig = Configuration()
        broadcastConfig.set("fs.defaultFS", config.hdfs.uri)
        return broadcastConfig
    }
}
