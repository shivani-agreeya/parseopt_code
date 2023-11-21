package com.verizon.reports

object ArgsListDemo {
  
  
  def main(args: Array[String]): Unit = {
    //[prefilenames=a.seq,b.seq postfilenames=c.seq,d.seq curDate=04-22-2019]
    if(args.length>0){
      val preFileNamesString=args(0).split("=")(1)
      println(preFileNamesString)
      
      
       val postFileNamesString=args(1).split("=")(1)
       println(postFileNamesString)
       
       
    }else{
      
    }
  }
}