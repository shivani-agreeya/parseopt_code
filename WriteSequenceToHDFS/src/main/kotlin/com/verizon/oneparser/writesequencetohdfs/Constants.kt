package com.verizon.oneparser.writesequencetohdfs

object LogStatus {
    const val LOG_MOVED_TO_HDFS = "moved to hdfs"
    const val LOG_REPROCESS = "reprocess"
    const val SEQ_IN_PROGRESS = "Sequence Inprogress"
    const val SEQ_DONE = "Sequence Done"
    const val DLF_DOES_NOT_EXIST = "DLF does not exist"
}

object Constants {
    const val CONST_DRM_DLF = "drm_dlf"
    const val CONST_DML_DLF = "dml_dlf"
    const val CONST_SIG_DLF = "sig_dlf"
    const val CONST_SIG = "sig"
    const val CONST_DLF = "dlf"
}

enum class DirType {
    DLF, SEQ
}

enum class DataType {
    NUMBER, FLOAT, DOUBLE
}
