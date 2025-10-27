import java.text.SimpleDateFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import util.Protocol.{Commit, CommitGeo, CommitSummary}
import util.{CommitGeoParser, CommitParser}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern

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
    dummy_question(commitStream).print()

    /** Start the streaming environment. **/
    env.execute()
  }

  /** Dummy question which maps each commits to its SHA. */
  def dummy_question(input: DataStream[Commit]): DataStream[(String,String)] = {
    question_nine(input)
  }

  /**
    * Write a Flink application which outputs the sha of commits with at least 20 additions.
    * Output format: sha
    */
  def question_one(input: DataStream[Commit]): DataStream[String] = {
    input
      .filter(x => x.stats.isDefined && x.stats.orNull.additions >= 20)
      .map(_.sha)
  }
  /**
    * Write a Flink application which outputs the names of the files with more than 30 deletions.
    * Output format:  fileName
    */
  def question_two(input: DataStream[Commit]): DataStream[String] = ???

  /**
    * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
    * Output format: (fileExtension, #occurrences)
    */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = ???

  /**
    * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
    * Output format: (extension, status, count)
    */
    def question_four(
        input: DataStream[Commit]): DataStream[(String, String, Int)] = {
      input
        .flatMap(_.files
          .filter(x =>
            x.status.isDefined &&
              x.filename.isDefined &&
              (x.filename.get.endsWith(".js") || x.filename.get.endsWith(".py"))
          )
          .map(x =>
            (replaceWithExtension(x.filename.get),x.status.get, x.changes)
          )
        )
        .keyBy(x => (x._1, x._2))
        .sum(2)
    }
  def replaceWithExtension (input: String): String = {
    return if (input.endsWith(".js")) ".js" else ".py"
  }

  /**
    * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
    * Make use of a non-keyed window.
    * Output format: (date, count)
    */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[Commit](Time.days(1)) // WTF IS THIS LECTURERS
        {
          override def extractTimestamp(element: Commit): Long = {
            element.commit.committer.date.getTime
          }
        }
      )
      .map(c => (new SimpleDateFormat("dd-MM-yyyy").format(c.commit.committer.date), 1))
      .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
      .reduce((value1, value2) => (value1._1, value1._2 + value2._2))
  }

  /**
    * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
    * Compute every 12 hours the amount of small and large commits in the last 48 hours.
    * Output format: (type, count)
    */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] = ???

  /**
    * For each repository compute a daily commit summary and output the summaries
    * with more than 20 commits and at most 2 unique committers.
    * The CommitSummary case class is already defined.
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
  def question_seven(commitStream: DataStream[Commit]): DataStream[CommitSummary] = {
    commitStream
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[Commit](Time.days(1)) {
          override def extractTimestamp(element: Commit): Long = {
            element.commit.committer.date.getTime
          }
        }
      )
      .keyBy(commit => extractRepoName(commit.url))
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .apply { (key, window, elements, out: Collector[CommitSummary]) =>
        val commits = elements.toList

        val amountOfCommits = commits.size
        val committers = commits
          .map(_.commit.committer.name)
          .distinct
        val amountOfCommitters = committers.size
        val totalChanges = commits.map(getStats).sum

        val committerCounts = commits
          .map(commit => commit.commit.committer.name)
          .groupBy(identity)
          .map { case (name, list) => (name, list.size) }

        val maxCommits = if (committerCounts.nonEmpty)
          committerCounts.map { case (_, count) => count }.max
        else 0

        val topCommitters = committerCounts
          .filter(x => x._2 == maxCommits)
          .toList
          .map(x => x._1)
          .sorted
          .mkString(",")
        val dateStr = new SimpleDateFormat("dd-MM-yyyy").format(new java.util.Date(window.getStart))

        out.collect(CommitSummary(key, dateStr, amountOfCommits, amountOfCommitters, totalChanges, topCommitters))
      }
      .filter(x => x.amountOfCommits > 20 && x.amountOfCommitters <= 2)
  }

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
      geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = ???

  /**
    * Find all files that were added and removed within one day. Output as (repository, filename).
    *
    * Hint: Use the Complex Event Processing library (CEP).
    * Output format: (repository, filename)
    */

  def question_nine(inputStream: DataStream[Commit]): DataStream[(String, String)] = {

    val fileEvents = inputStream.flatMap { commit =>
      val repo = extractRepoName(commit.url)
      val timestamp = commit.commit.committer.date.getTime

      commit.files.collect {
        case file if file.filename.isDefined && file.status.isDefined =>
          (repo, file.filename.get, file.status.get, timestamp)
      }
    }

    // Define the CEP pattern: added followed by removed within 1 day
    val pattern = Pattern.begin[(String, String, String, Long)]("added")
      .where(_._3 == "added")
      .followedBy("removed")
      .where(_._3 == "removed")
      .within(Time.days(1))

    val patternStream = CEP.pattern(fileEvents.keyBy(_._2), pattern)

    patternStream.select(new PatternSelectFunction[(String, String, String, Long), (String, String)] {
      override def select(pattern: java.util.Map[String, java.util.List[(String, String, String, Long)]]) = {
        val addedEvent = pattern.get("added").get(0)
        (addedEvent._1, addedEvent._2) // (repository, filename)
      }
    })
  }


  def extractRepoName(url: String): String = {
    url.split("/").slice(4, 6).mkString("/")
  }
  def getStats(x : Commit): Int = {
    if (x.stats.isDefined) x.stats.get.total else 0
  }
}
