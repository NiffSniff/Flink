import java.text.SimpleDateFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import util.Protocol.{Commit, CommitGeo, CommitSummary, Stats}
import util.{CommitGeoParser, CommitParser}

/** Do NOT rename this class, otherwise autograding will fail. **/
object FlinkAssignment {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    /**
      * Setups the streaming environment including loading and parsing of the datasets.
      *
      * DO NOT TOUCH!
      */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read and parses commit stream.
    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    // Read and parses commit geo stream.
    val commitGeoStream =
      env
        .readTextFile("data/flink_commits_geo.json")
        .map(new CommitGeoParser)

    /** Use the space below to print and test your questions. */
    dummy_question(commitStream, commitGeoStream).print()

    /** Start the streaming environment. **/
    env.execute()
  }

  /** Dummy question which maps each commits to its SHA. */
  def dummy_question(input: DataStream[Commit], geo: DataStream[CommitGeo]): DataStream[(String, Int)] = {
    question_eight(input, geo)
  }

  /**
    * Write a Flink application which outputs the sha of commits with at least 20 additions.
    * Output format: sha
    */
  def question_one(input: DataStream[Commit]): DataStream[String] = ???

  /**
    * Write a Flink application which outputs the names of the files with more than 30 deletions.
    * Output format:  fileName
    */
  def question_two(input: DataStream[Commit]): DataStream[String] = input
    .flatMap(x => x.files)
    .filter(x => x.deletions > 30)
    .map(x => x.filename.getOrElse("unknown"))

  /**
    * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
    * Output format: (fileExtension, #occurrences)
    */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = input
    .flatMap(x => x.files)
    .map(x => x.filename.getOrElse("unknown").split("\\.").last)
    .filter(x => x == "java" || x == "scala")
    .map(x => (x, 1))
    .keyBy(_._1)
    .sum(1)

  /**
    * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
    * Output format: (extension, status, count)
    */
  def question_four(
      input: DataStream[Commit]): DataStream[(String, String, Int)] = ???

  /**
    * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
    * Make use of a non-keyed window.
    * Output format: (date, count)
    */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = ???

  /**
    * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
    * Compute every 12 hours the amount of small and large commits in the last 48 hours.
    * Output format: (type, count)
    */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] = input
    .assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[Commit](Time.hours(12))
      {
        override def extractTimestamp(element: Commit): Long = {
          element.commit.committer.date.getTime
        }
      }
    )
    .map(x => if (x.stats.getOrElse(Stats(0, 0, 0)).total <= 20) "small"  else "large")
    .map(x => (x, 1))
    .keyBy(x => x._1)
    .window(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
    .sum(1)

  /**
    * For each repository compute a daily commit summary and output the summaries with more than 20 commits and at most 2 unique committers. The CommitSummary case class is already defined.
    *
    * The fields of this case class:
    *
    * repo: name of the repo.
    * date: use the start of the window in format "dd-MM-yyyy".
    * amountOfCommits: the number of commits on that day for that repository.
    * amountOfCommitters: the amount of unique committers contributing to the repository.
    * totalChanges: the sum of total changes in all commits.
    * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
    *
    * Hint: Write your own ProcessWindowFunction.
    * Output format: CommitSummary
    */
  def question_seven(
      commitStream: DataStream[Commit]): DataStream[CommitSummary] = ???

  /**
    * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
    * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
    * Get the weekly amount of changes for the java files (.java extension) per continent. If no java files are changed in a week, no output should be shown that week.
    *
    * Hint: Find the correct join to use!
    * Output format: (continent, amount)
    */
  def question_eight(
      commitStream: DataStream[Commit],
      geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = commitStream
    .assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[Commit](Time.hours(12))
      {
        override def extractTimestamp(element: Commit): Long = {
          element.commit.committer.date.getTime
        }
      }
    )
    .keyBy(x => x.sha)
    .intervalJoin(geoStream
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[CommitGeo](Time.hours(12))
        {
          override def extractTimestamp(element: CommitGeo): Long = {
            element.createdAt.getTime
          }
        }
      )
      .keyBy(x => x.sha))
    .between(Time.hours(-1), Time.minutes(30))
    .process(new ProcessJoinFunction[Commit, CommitGeo, (Commit, CommitGeo)] {
      override def processElement(
                                   commit: Commit,
                                   commitGeo: CommitGeo,
                                   ctx: ProcessJoinFunction[Commit, CommitGeo, (Commit, CommitGeo)]#Context,
                                   out: Collector[(Commit, CommitGeo)]): Unit = {
        out.collect((commit, commitGeo))
      }
    })
    .map(x => (x._2.continent, x._1.files
      .filter(x => x.filename.getOrElse("unknown").split("\\.").last == "java")
      .map(x => x.changes).sum))
    .keyBy(x => x._1)
    .window(TumblingEventTimeWindows.of(Time.days(7)))
    .sum(1)
    .filter(x => x._2 != 0)

  /**
    * Find all files that were added and removed within one day. Output as (repository, filename).
    *
    * Hint: Use the Complex Event Processing library (CEP).
    * Output format: (repository, filename)
    */
  def question_nine(
      inputStream: DataStream[Commit]): DataStream[(String, String)] = ???

}
