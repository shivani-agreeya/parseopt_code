package com.verizon.reports.dto

case class GetInputReportDto( userId :Long,
                              prefileName: String,
                              postfileName: String,
                              prefileNames: List[String],
                              postfileNames: List[String],
                              dmTimeStamp: String,
                              startTime: String,
                              endTime: String,
                              preTimeFrom:String,
	                            preTimeTo:String,
	                            postTimeFrom:String,
	                            postTimeTo:String,
	                            preFileTestId: String,
	                            postFileTestId: String)

