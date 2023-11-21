package com.verizon.oneparser.common

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import java.util.Base64

import com.typesafe.scalalogging.LazyLogging


object SecurityUtil extends LazyLogging{

  private val Algorithm = "AES/CBC/PKCS5Padding"
  private val Key = new SecretKeySpec(Base64.getDecoder.decode("DxVnlUlQSu3E5acRu7HPwg=="), "AES")
  private val IvSpec = new IvParameterSpec(new Array[Byte](16))

  def encrypt(text: String): String = {
    var output = ""
    try {
      val cipher = Cipher.getInstance(Algorithm)
      cipher.init(Cipher.ENCRYPT_MODE, Key, IvSpec)
      output = new String(Base64.getEncoder.encode(cipher.doFinal(text.getBytes("utf-8"))), "utf-8")
    }
    catch {
      case ge:Exception =>{
        logger.info("Exception occurred while Encryption : "+ge.printStackTrace)
      }
    }
    output
  }

  def decrypt(text: String): String = {
    var output = ""
    try {
    val cipher = Cipher.getInstance(Algorithm)
    cipher.init(Cipher.DECRYPT_MODE, Key, IvSpec)
    output = new String(cipher.doFinal(Base64.getDecoder.decode(text.getBytes("utf-8"))), "utf-8")
    }
    catch {
      case ge:Exception =>{
        logger.info("Exception occurred while Decryption : "+ge.printStackTrace)
      }
    }
    output
  }

}
