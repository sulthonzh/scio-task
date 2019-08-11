package ai.kata.sciotask

import com.spotify.scio._
import com.spotify.scio.sql._
import com.spotify.scio.jdbc._

import com.twitter.algebird.{Aggregator, Semigroup}

import scala.util.control.NonFatal
import scala.util.Try

import org.joda.time.{DateTimeZone, Duration, Instant, LocalDate, LocalDateTime, LocalTime}

object AverageSummary {
  case class Entry(id: String, environmentId: String, channelId: String, sessionId: String, duration: Double, createdAt: Instant)
  case class Counter(environmentId: String, channelId: String, hour: Int, duration: Double)
  case class Result(timestamp: Long, environmentId: String, channelId: String, hour: Int, duration: Double)

  def parseEvent(line: String): Option[Entry] =
    Try {
      val t = line.split(",")
      Entry(
        t(0).stripSuffix("\"").stripPrefix("\""), 
        t(1).stripSuffix("\"").stripPrefix("\""), 
        t(2).stripSuffix("\"").stripPrefix("\""), 
        t(3).stripSuffix("\"").stripPrefix("\""), 
        t(4).stripSuffix("\"").stripPrefix("\"").toDouble, 
        new Instant(t(5).stripSuffix("\"").stripPrefix("\"").toLong)
      )
    }.toOption

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val exampleData = "dummy.csv"
    val input = args.getOrElse("input", exampleData)
    val connOptions = getConnectionOptions()
    val averageWriteOptions = getAverageWriteOptions(connOptions)
    val summaryWriteOptions = getSummaryWriteOptions(connOptions)

    val entry = sc.textFile(input)
      .flatMap(AverageSummary.parseEvent)

    val tempEntry = entry
      .map(i => Counter(i.environmentId, i.channelId, i.createdAt.toDateTime().getHourOfDay(), i.duration))
    
    tsql"select `environmentId`, `channelId`, `hour`, SUM(`duration`) from $tempEntry group by `environmentId`, `channelId`, `hour`"
      .as[(String, String, Int, Double)]
      .map(t => Result(Instant.now().toDateTime().getMillis(), t._1, t._2, t._3, t._4))
      .saveAsJdbc(summaryWriteOptions)

    tsql"select `environmentId`, `channelId`, `hour`, AVG(`duration`) from $tempEntry group by `environmentId`, `channelId`, `hour`"
      .as[(String, String, Int, Double)]
      .map(t => Result(Instant.now().toDateTime().getMillis(), t._1, t._2, t._3, t._4))
      .saveAsJdbc(averageWriteOptions)
    
    sc.run()
    ()
  }

  def getConnectionOptions(): JdbcConnectionOptions =
    JdbcConnectionOptions(
      username = "root",
      password = Some("root"),
      driverClass = classOf[com.mysql.cj.jdbc.Driver],
      connectionUrl = "jdbc:mysql://root:root@localhost:3306/kata_ai?charset=utf8"
    )

  def getSummaryWriteOptions(connOpts: JdbcConnectionOptions): JdbcWriteOptions[Result] =
  JdbcWriteOptions(
    connectionOptions = connOpts,
    statement = "INSERT INTO kata_ai.summary (`timestamp`, `environment_id`, `channel_id`, `hour`, `summary_duration`) VALUES(?, ?, ?, ?, ?);",
    preparedStatementSetter = (kv, s) => {
      s.setLong(1, kv.timestamp)
      s.setString(2, kv.environmentId)
      s.setString(3, kv.channelId)
      s.setInt(4, kv.hour)
      s.setDouble(5, kv.duration)
    }
  )

  def getAverageWriteOptions(connOpts: JdbcConnectionOptions): JdbcWriteOptions[Result] =
  JdbcWriteOptions(
    connectionOptions = connOpts,
    statement = "INSERT INTO kata_ai.average (`timestamp`, `environment_id`, `channel_id`, `hour`, `avg_duration`) VALUES(?, ?, ?, ?, ?);",
    preparedStatementSetter = (kv, s) => {
      s.setLong(1, kv.timestamp)
      s.setString(2, kv.environmentId)
      s.setString(3, kv.channelId)
      s.setInt(4, kv.hour)
      s.setDouble(5, kv.duration)
    }
  )

}