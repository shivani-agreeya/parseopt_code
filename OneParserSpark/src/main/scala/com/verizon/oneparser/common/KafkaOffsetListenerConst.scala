package com.verizon.oneparser.common

object KafkaOffsetListenerConst {
  val CONFIG_KEY_GROUP_ID = "consumer.commit.groupid"
  val CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION = "kafka." + CONFIG_KEY_GROUP_ID
}
