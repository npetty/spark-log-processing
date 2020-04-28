package my.challenge

import java.text.SimpleDateFormat

/**
 *
 * @param visitor Host name or IP in the entry
 * @param clientIdentity In these logs, always "-"
 * @param user In these logs, always "-"
 * @param dateTimeStr Timestamp string in format [20/Jul/1995:15:28:50 -0400]
 * @param day Generated value of the day in format "yyyyMMdd" for grouping
 * @param request GET,HEAD,POST, etc
 * @param url Requested URL for this entry
 * @param statusCode HTML status code
 * @param bytesSent How many bytes were transferred
 */
class LogEntry_b(
   visitor: String,
   clientIdentity: String,
   user: String,
   dateTimeStr: String,
   day: String,
   request:String,
   url:String,
   statusCode:Int,
   bytesSent:Long
 ){
 val LOG_PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+)\s[+\-]\d{4}\] "(GET|HEAD|POST)\s([\S|\s]+?)\s?(HTTP\/[0|1]\.[0-2])?" (\d{3}) (\S+)""".r

 // Use this as hostname when an invalid log is found
 val INVALID_CLIENT = "Empty"

 // Slightly modified because we really don't care about the TZ adjustment portion
 val logDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss")

 // Format for the generated column to partition the day
 val outputDayFormat = new SimpleDateFormat("yyyyMMdd")


}



