package com.verizon.oneparser.common

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import javax.mail.{Message, Session}
import javax.mail.internet.{InternetAddress, MimeMessage}

object Mailer extends LazyLogging{
  val defaultPwd:String="JhxZfJED9fpv8/Fr5bPSKQ=="
  val isSSLOn:Boolean = false
  def sendMail(emailDto: EmailDto) = {
    val properties = new Properties()
    properties.put("mail.smtp.port", emailDto.smtpPort)
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")

    val session = Session.getDefaultInstance(properties, null)

    val message = new MimeMessage(session)
    message.addRecipient(Message.RecipientType.TO, new InternetAddress(emailDto.recipientAddress));
    message.setSubject(emailDto.subject)
    message.setContent(emailDto.emailContent, "text/html")
    message.setFrom(new InternetAddress(emailDto.emailFrom))

    val transport = session.getTransport("smtp")
    transport.connect(emailDto.hostName, emailDto.userName, if(emailDto.password.nonEmpty) emailDto.password else SecurityUtil.decrypt(defaultPwd))
    transport.sendMessage(message, message.getAllRecipients)
  }

  case class EmailDto(hostName:String="",userName:String="",password:String="",emailFrom:String="",senderName:String="",smtpPort:Integer=0,isSSLOn:Boolean=false,emailContent:String="",recipientAddress:String="",subject:String="")
  case class UserAndFileInfo(testId:Integer=0,fileName:String="", userId:Integer=0,userEmail:String="",firstName:String="", lastName:String="",odProcess:Boolean=false, reprocess:Boolean=false)

  /*def main(args:Array[String]) = {
    logger.info("Mailer Main Method call")
    var emailDto:EmailDto = EmailDto()
    val userInfo:UserAndFileInfo = UserAndFileInfo(fileName = "abc.dlf",userEmail = "srinivasulu@vzwdt.com",odProcess = true)
    emailDto = emailDto.copy(hostName = "vzwdt.com",userName = "DMATBDSupport@vzwdt.com",smtpPort = 25, emailFrom = "DMATDevSupport@vzwdt.com",
      senderName = "DMATDevSupport",password = "Summer2017@")
    sendEmailToUser(userInfo,emailDto)
  }*/
  def sendEmailToUser(userAndFileInfo: UserAndFileInfo, emailDto: EmailDto): Unit ={
    val emailTemplate = buildEmailTemplate(userAndFileInfo)
    var updatedEmailDto = emailDto;
    val emailSubject = if(userAndFileInfo.odProcess) "Your On-Demand file processing has been completed!" else "Your file has been Re-Processed!"
    updatedEmailDto = updatedEmailDto.copy(recipientAddress = userAndFileInfo.userEmail,emailContent = emailTemplate,subject = emailSubject)
    logger.info("Email Sending to : "+updatedEmailDto.recipientAddress)
    sendMail(updatedEmailDto)
  }
  def buildEmailTemplate(userAndFileInfo: UserAndFileInfo): String ={
    val requestType = if(userAndFileInfo.odProcess)"ON-DEMAND file processing to" else "to REPROCESS"
    val emailTemplate = "<!DOCTYPE html>\n" +
      "<html lang=\"en\" xmlns=\"http://www.w3.org/1999/xhtml\" >\n" +
      "    <head></head>\n" +
      "    <body>\n" +
      "        <table style='width:100.0%;background:#EEEEEE;border-spacing:0'>\n" +
      "            <tr>\n" +
      "                <td style='padding:15.0pt 15.0pt 15.0pt 15.0pt'>\n" +
      "                    <table style='width:100.0%;background:white;border-spacing:0'>\n" +
      "                        <tr>\n" +
      "                            <td valign='top' style='padding:15.0pt 15.0pt 15.0pt 15.0pt'>\n" +
      "                                <span>\n" +
      "                                        <span style='text-decoration:none;cursor:default;'>\n" +
      "                                            <img style='cursor:default;' border='0' width='163' height='36' id=\"_x0000_i1025\"\n" +
      "                                                src=\"https://vzwdt.com/DMAT/src/images/ic_dmat_header_b.png\"\n" +
      "                                                alt=\"Verizon logo\"/>\n" +
      "                                        </span>\n" +
      "                                </span>\n" +
      "                            </td>\n" +
      "                        </tr>\n" +
      "                        <tr>\n" +
      "                            <td valign='top' style='border:none;border-bottom:solid #DDDDDD 1.0pt;\n" +
      "                                padding:0in 15.0pt 15.0pt 15.0pt'>                                   \n" +
      "                                <p/>\n" +
      "                            </td>\n" +
      "                        </tr>\n" +
      "                        <tr>\n" +
      "                            <td style='padding:15.0pt 15.0pt 15.0pt 15.0pt'>\n" +
      "                                <table style='width:100.0%;border-spacing:0'>\n" +
      "                                    <tr>\n" +
      "                                        <td style='background:#D2F3D8;padding:7.5pt 7.5pt 7.5pt 7.5pt'>\n" +
      "                                            <span style='color:#29562B;font-size:10.5pt;font-family:\"Arial\",sans-serif'>\n" +
      "                                                <b>"+userAndFileInfo.fileName+"</b> file has been processed.\n" +
      "                                            </span>\n" +
      "                                        </td>\n" +
      "                                    </tr>\n" +
      "                                </table>\n" +
      "                            </td>\n" +
      "                        </tr>\n" +
      "                        <tr>\n" +
      "                            <td style='padding:7.5pt 15.0pt 7.5pt 15.0pt'></td>\n" +
      "                        </tr>\n" +
      "                        <tr>\n" +
      "                            <td style='padding:0in 15.0pt 15.0pt 15.0pt'>\n" +
      "                                <span style='font-size:10.5pt;font-family:\"Arial\",sans-serif;\n" +
      "                                    color:#333333'> Hi "+userAndFileInfo.firstName+",\n" +
      "                                </span>\n" +
      "                            </td>\n" +
      "                        </tr>\n" +
      "                        <tr>\n" +
      "                            <td style='padding:0in 15.0pt 15.0pt 15.0pt'>\n" +
      "                                <span style='font-size:10.5pt;font-family:\"Arial\",sans-serif;\n" +
      "                                    color:#333333'>\n" +
      "                                    You have requested "+requestType+" a LOG file from DMAT Log Manager. It has been processed successfully and available for MAP analysis. \n" +
      "                                </span>\n" +
      "                            </td>\n" +
      "                        </tr>\n" +
      "                        <tr>\n" +
      "                            <td style='padding:0in 15.0pt 15.0pt 15.0pt'>\n" +
      "                                <p style='margin-top:0in;margin-right:0in;margin-bottom:12.0pt;margin-left:\n" +
      "                                    0in'>\n" +
      "                                    <span style='font-size:10.5pt;font-family:\"Arial\",sans-serif;\n" +
      "                                        color:#333333'>\n" +
      "                                        Thank You,<br/>\n" +
      "                                        <span>Device Technology Team</span>\n" +
      "                                    </span>\n" +
      "                                </p>\n" +
      "                            </td>\n" +
      "                        </tr>\n" +
      "                        <tr>\n" +
      "                            <td style='border:none;border-top:solid #DDDDDD 1.0pt; padding:7.5pt 15.0pt 15.0pt 15.0pt'>\n" +
      "                                <p align='center' style='text-align:center'>\n" +
      "                                    <span\n" +
      "                                        style='font-size:9.0pt;font-family:\"Arial\",sans-serif;color:#959595'>\n" +
      "                                        This mail is an auto-generated mail from Device Technology<br/>\n" +
      "                                        Please do not reply to this email message.\n" +
      "                                    </span>\n" +
      "                                </p>\n" +
      "                            </td>\n" +
      "                        </tr>\n" +
      "                    </table>\n" +
      "                </td>\n" +
      "            </tr>\n" +
      "        </table>\n" +
      "    </body>\n" +
      "</html>";
    emailTemplate
  }

}
