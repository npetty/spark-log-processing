package my.challenge

import java.text.SimpleDateFormat

import org.slf4j.{Logger, LoggerFactory}

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
case class LogEntry(
   visitor: String,
   clientIdentity: String,
   user: String,
   dateTimeStr: String,
   day: String,
   request:String,
   url:String,
   statusCode:Int,
   bytesSent:Long
 )

/**
 *  This object and its companion class above represent a single log entry. Most of the logic is implemented
 *  as a regular expression that is used to capture the relevant fields of the entry in matching groups.
 *  If the regex does not match, the record is treated as invalid.
 *
 *
 *  Example log entry
 *  199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085
 */
object LogEntry {

  // Matching group distro
  //                      1     2     3         4                          5              6              7                    8      9
  val LOG_PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+)\s[+\-]\d{4}\] "(GET|HEAD|POST)\s([\S|\s]+?)\s?(HTTP\/[0|1]\.[0-2])?" (\d{3}) (\S+)""".r

  // Use this as hostname when an invalid log is found
  val INVALID_CLIENT = "Empty"

  // Slightly modified because we really don't care about the TZ adjustment portion
  val logDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss")

  // Format for the generated column to partition the day
  val outputDayFormat = new SimpleDateFormat("yyyyMMdd")

  def log : Logger = LoggerFactory.getLogger( getClass )

  /**
   *
   * @param logEntry Textual log entry string
   * @return LogEntry object structured fields and parsed day value
   */
  def parseAccessLogEntry(logEntry: String): LogEntry = {
    try {
      val res = LOG_PATTERN.findFirstMatchIn(logEntry)
      //If no match, reject it and log the rejection
      if(res.isEmpty) {
        log.warn("Rejected Log Line: " + logEntry)
        LogEntry(INVALID_CLIENT, "", "", "", "", "", "", -1, -1)
      }
      else {
        val m = res.get
        // NOTE: HEAD does not have a content size. We don't want to fail on parsing an invalid int if we don't need to.
        if (m.group(9).equals("-")) {
          LogEntry(m.group(1), m.group(2), m.group(3), m.group(4), outputDayFormat.format(logDateFormat.parse(m.group(4))),
            m.group(5), m.group(6), m.group(8).toInt, 0)
        }
        // parse the most valid entries
        else {
          LogEntry(m.group(1), m.group(2), m.group(3), m.group(4), outputDayFormat.format(logDateFormat.parse(m.group(4))),
            m.group(5), m.group(6), m.group(8).toInt, m.group(9).toLong)
        }
      }
    } catch {
      //If an exception is encountered reject the line and log it.
      case e: Exception =>
        log.warn("Exception on line:" + logEntry + ":" + e.getMessage);
        LogEntry(INVALID_CLIENT, "", "", "", "",  "", "", -1, -1)
    }
  }
}
