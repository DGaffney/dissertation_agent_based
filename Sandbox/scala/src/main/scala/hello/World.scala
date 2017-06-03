//To ask for object methods
//days.getClass.getMethods.map(_.getName)
//package hello
Runtime.getRuntime().maxMemory()
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import java.io.File
import com.github.tototoshi.csv._
import scala.collection.mutable.{Map, SynchronizedMap, HashMap}
import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.json4s._
import org.json4s.native.JsonMethods._
import java.nio.charset.StandardCharsets._
import java.nio.file.{Files, Paths}
import scala.util.Random
import org.json4s.native.Serialization.{read, write}
val r = scala.util.Random
def getListOfFiles(dir: String):List[File] = {
  val d = new File(dir)
  if (d.exists && d.isDirectory) {
    d.listFiles.filter(_.isFile).toList
  } else {
    List[File]()
  }
}
implicit val formats = DefaultFormats
case class SubredditEdges(subreddit: String, edges: Array[String])
var days = getListOfFiles("../larger_data/user_counts").sorted
var world = new HashMap[Symbol, ArrayBuffer[Symbol]]//
var user_counts = new HashMap[Symbol, Int]//
var last_position = new HashMap[Symbol, Symbol]
var self_loop_percent = new HashMap[Symbol, Float]//
var subreddit_counts = new HashMap[Symbol, Int]
def updateUserCounts(day: String):HashMap[Symbol, Int] = {
  var new_user_counts = new HashMap[Symbol, Int]
  CSVReader.open(new File("../larger_data/user_counts/"+day)).foreach(user_count => new_user_counts(Symbol(user_count(0))) = user_count(1).toInt)
  new_user_counts
}
def updateSelfLoopPcts(day: String):HashMap[Symbol, Float] = {
  var new_self_loops = new HashMap[Symbol, Float]
  CSVReader.open(new File("../larger_data/accumulated_self_loops/"+day)).foreach(user_count => new_self_loops(Symbol(user_count(0))) = user_count(1).toFloat)
  new_self_loops
}
def updateWorld(day: String, world: HashMap[Symbol, ArrayBuffer[Symbol]]):HashMap[Symbol, ArrayBuffer[Symbol]] = {
  var world_updates = (parse(new String(Files.readAllBytes(Paths.get("../larger_data/cumulative_daily_nets_new_scalaed2/"+day)), UTF_8)) \ "subreddit_edges").extract[List[SubredditEdges]]
  for (subreddit <- world_updates) {
    var sub_sym = Symbol(subreddit.subreddit)
    if (!world.contains(sub_sym)){
      world(sub_sym) = ArrayBuffer()
    }
    for (edge <- subreddit.edges){
      var sym_edge = Symbol(edge)
      world(sub_sym) += sym_edge
    }
  }
  world
}
def updateLastPositions(day: String):HashMap[Symbol, Symbol] = {
  var new_user_starts = new HashMap[Symbol, Symbol]
  CSVReader.open(new File("../larger_data/user_starts/"+day)).foreach(user_count => new_user_starts(Symbol(user_count(0))) = Symbol(user_count(1)))
  new_user_starts
}
def walk(user: Symbol, count: Int, startNode: Symbol, world: HashMap[Symbol, ArrayBuffer[Symbol]], self_loop_percent: HashMap[Symbol, Float]):Tuple2[Symbol, ArrayBuffer[Symbol]] = {
  var history = ArrayBuffer[Symbol]()
  var node = startNode
  1 to count foreach {_ => 
    if (!self_loop_percent.contains(node) || r.nextFloat < self_loop_percent(node) || !world.contains(node)){
      history += node
    } else {
      var nextStep = world(node)(Random.nextInt(world(node).size))
      history += nextStep
      node = nextStep
    }
  }
  (user, history)
}
def update(histories: List[(Symbol, ArrayBuffer[Symbol])], subreddit_counts: HashMap[Symbol, Int]):Tuple2[HashMap[Symbol, Int], HashMap[Symbol, Symbol]] = {
  var updated_last_positions = new HashMap[Symbol, Symbol]
  for (user_history <- histories){
    var user = user_history._1
    var history = user_history._2
    updated_last_positions(user) = history.last
    for (subreddit <- history){
      if (!subreddit_counts.contains(subreddit)){
        subreddit_counts(subreddit) = 0
      }
      subreddit_counts(subreddit) += 1
    }
  }
  (subreddit_counts, updated_last_positions)
}

object Simulation extends App {
  for (day <- days){
    var day_str = day.getName
    println(day_str)
    println("Update User Counts")
    user_counts = updateUserCounts(day_str)
    println("Update Self Loops")
    self_loop_percent = self_loop_percent ++ updateSelfLoopPcts(day_str)
    println("Update Last Positions for new Users")
    last_position = last_position ++ updateLastPositions(day_str)
    println("Update World")
    world = updateWorld(day_str, world)
    println("Run Walk")
    var histories = user_counts.par.map(keyVal => walk(keyVal._1, keyVal._2, last_position(keyVal._1), world, self_loop_percent))
    println("Update Stats")
    var stats = update(histories.toList, subreddit_counts)
    println("Update Positions")
    last_position = last_position ++ stats._2
    println("Total Transits:")
    println(subreddit_counts.values.sum)
  }
  Files.write(Paths.get("scala_random_walk_subreddit_counts"+r.nextInt(1000000).toString+".txt"), write(subreddit_counts).getBytes(StandardCharsets.UTF_8))
}
