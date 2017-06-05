//To ask for object methods
//days.getClass.getMethods.map(_.getName)
package hello
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
import org.scalautils._
import java.util.TreeMap
import com.cra.figaro.util._
//var rr = new MapResampler(List(10000.0 -> "1", 1000.0 -> "2", 100.0 -> "3"))
//Vector.fill((11100*10))(rr.resample()).groupBy(identity).mapValues(_.size)
//var big_list = List.fill((400000))(((scala.util.Random.nextFloat*100000).toDouble, (scala.util.Random.nextFloat*100000).toInt.toString))
//var rr = [new MapResampler(big_list), new MapResampler(big_list)]
//Vector.fill((10))(new MapResampler(big_list).resample()).groupBy(identity).mapValues(_.size)
// 1 to 20 foreach {_ =>
//   days = getListOfFiles("../larger_data/user_counts").sorted
//   world = new HashMap[Symbol, ArrayBuffer[Symbol]]//
//   user_counts = new HashMap[Symbol, Int]//
//   last_position = new HashMap[Symbol, Symbol]
//   self_loop_percent = new HashMap[Symbol, Float]//
//   subreddit_counts = new HashMap[Symbol, Int]
// }
object Simulation extends App {
  implicit val formats = DefaultFormats
  case class SubredditEdges(subreddit: String, edges: Array[String])
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
  def updateWeightModels(weight_type: String, world: HashMap[Symbol, ArrayBuffer[Symbol]], subreddit_counts: HashMap[Symbol, Int]):HashMap[Symbol, com.cra.figaro.util.MapResampler[String]] = {
    var weight_models = new HashMap[Symbol, com.cra.figaro.util.MapResampler[String]]
    var nodes = world.keys
    for (node <- nodes) {
      if (world(node).size != 0){
        if (weight_type == "log"){
          var weights = world(node).map(keyVal => (Math.log((if (subreddit_counts(keyVal) == 0) 1 else subreddit_counts(keyVal)).toDouble)+1, keyVal.name)).toList
          var model = new MapResampler(weights)
          weight_models(node) = model
        } else if (weight_type == "proportional") {
          var weights = world(node).map(keyVal => ((if (subreddit_counts(keyVal) == 0) 1 else subreddit_counts(keyVal)).toDouble+1, keyVal.name)).toList
          var model = new MapResampler(weights)
          weight_models(node) = model
        } else if (weight_type == "uniform") {
          var weights = world(node).map(keyVal => (1.toDouble, keyVal.name)).toList
          var model = new MapResampler(weights)
          weight_models(node) = model
        }
      }
    }
    weight_models
  }
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
    var world_updates = (parse(new String(Files.readAllBytes(Paths.get("../larger_data/cumulative_daily_nets/"+day)), UTF_8)) \ "subreddit_edges").extract[List[SubredditEdges]]
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
  def walk(user: Symbol, count: Int, startNode: Symbol, world: HashMap[Symbol, ArrayBuffer[Symbol]], self_loop_percent: HashMap[Symbol, Float], weight_samples: HashMap[Symbol, com.cra.figaro.util.MapResampler[String]]):Tuple2[Symbol, ArrayBuffer[Symbol]] = {
    var history = ArrayBuffer[Symbol]()
    var node = startNode
    1 to count foreach {_ => 
      if (!self_loop_percent.contains(node) || !weight_samples.contains(node)|| r.nextFloat < self_loop_percent(node) || !world.contains(node)){
        history += node
      } else {
        // var nextStep = world(node)(Random.nextInt(world(node).size))
        var nextStep = Symbol(weight_samples(node).resample())
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
  var walk_types = List("log", "proportional", "uniform")
  var start_time = System.currentTimeMillis()
  val r = scala.util.Random
  var days = getListOfFiles("../larger_data/user_counts").sorted
  var world = new HashMap[Symbol, ArrayBuffer[Symbol]]//
  var user_counts = new HashMap[Symbol, Int]//
  var last_position = new HashMap[Symbol, Symbol]
  var self_loop_percent = new HashMap[Symbol, Float]//
  var subreddit_counts = new HashMap[Symbol, Int]  { override def default(key:Symbol) = 0 }
  var weight_samples = updateWeightModels("none", world, subreddit_counts)
  1 to 100 foreach {c =>
    var weight_type = walk_types(c%3)
    for (day <- days.take(2000)){
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
      var histories = user_counts.par.map(keyVal => walk(keyVal._1, keyVal._2, last_position(keyVal._1), world, self_loop_percent, weight_samples))
      println("Update Stats")
      var stats = update(histories.toList, subreddit_counts)
      println("Update Positions")
      last_position = last_position ++ stats._2
      weight_samples = updateWeightModels(weight_type, world, subreddit_counts)
      println("Total Transits:")
      println(subreddit_counts.values.sum)
    }
    Files.write(Paths.get("scala_random_walk_subreddit_counts_"+weight_type+"_"+r.nextInt(1000000).toString+".txt"), write(subreddit_counts).getBytes(StandardCharsets.UTF_8))
    var end_time = System.currentTimeMillis()
    println("Took "+((end_time - start_time)/1000).toString+" seconds to run!")
    start_time = System.currentTimeMillis()
    days = getListOfFiles("../larger_data/user_counts").sorted
    world = new HashMap[Symbol, ArrayBuffer[Symbol]]//
    user_counts = new HashMap[Symbol, Int]//
    last_position = new HashMap[Symbol, Symbol]
    self_loop_percent = new HashMap[Symbol, Float]//
    subreddit_counts = new HashMap[Symbol, Int]  { override def default(key:Symbol) = 0 }
    weight_samples = updateWeightModels("none", world, subreddit_counts)
  }
}