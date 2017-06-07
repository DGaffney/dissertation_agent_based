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
  val r = scala.util.Random
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
          var weights = world(node).map(keyVal => (Math.log10((if (subreddit_counts(keyVal) == 0) 1 else subreddit_counts(keyVal)).toDouble)+1, keyVal.name)).toList
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
        } else if (weight_type == "sqrt") {
          var weights = world(node).map(keyVal => ((if (subreddit_counts(keyVal) == 0) 1 else scala.math.pow(subreddit_counts(keyVal),1/2.0)).toDouble+1, keyVal.name)).toList
          var model = new MapResampler(weights)
          weight_models(node) = model
        } else if (weight_type == "third_root") {
          var weights = world(node).map(keyVal => ((if (subreddit_counts(keyVal) == 0) 1 else scala.math.pow(subreddit_counts(keyVal),1/3.0)).toDouble+1, keyVal.name)).toList
          var model = new MapResampler(weights)
          weight_models(node) = model
        } else if (weight_type == "fourth_root") {
          var weights = world(node).map(keyVal => ((if (subreddit_counts(keyVal) == 0) 1 else scala.math.pow(subreddit_counts(keyVal),1/4.0)).toDouble+1, keyVal.name)).toList
          var model = new MapResampler(weights)
          weight_models(node) = model
        } else if (weight_type == "sublinear1") {
          var weights = world(node).map(keyVal => ((if (subreddit_counts(keyVal) == 0) 1 else scala.math.pow(subreddit_counts(keyVal),0.1)).toDouble+1, keyVal.name)).toList
          var model = new MapResampler(weights)
          weight_models(node) = model
        } else if (weight_type == "sublinear2") {
          var weights = world(node).map(keyVal => ((if (subreddit_counts(keyVal) == 0) 1 else scala.math.pow(subreddit_counts(keyVal),0.2)).toDouble+1, keyVal.name)).toList
          var model = new MapResampler(weights)
          weight_models(node) = model
        } else if (weight_type == "sublinear3") {
          var weights = world(node).map(keyVal => ((if (subreddit_counts(keyVal) == 0) 1 else scala.math.pow(subreddit_counts(keyVal),0.3)).toDouble+1, keyVal.name)).toList
          var model = new MapResampler(weights)
          weight_models(node) = model
        } else if (weight_type == "sublinear4") {
          var weights = world(node).map(keyVal => (scala.math.pow(subreddit_counts(keyVal),0.4).toDouble, keyVal.name)).toList
          var model = new MapResampler(weights)
          weight_models(node) = model
        } else if (weight_type == "sublinear5") {
          var weights = world(node).map(keyVal => (scala.math.pow(subreddit_counts(keyVal),0.5).toDouble, keyVal.name)).toList
          var model = new MapResampler(weights)
          weight_models(node) = model
        } else if (weight_type == "sublinear6") {
          var weights = world(node).map(keyVal => (scala.math.pow(subreddit_counts(keyVal)+1,0.6).toDouble, keyVal.name)).toList
          var sumval = weights.map(v => v._1).sum
          var baseline = 1.0/weights.size
          var model = new MapResampler(weights.map(keyVal => (keyVal._1/sumval+baseline, keyVal._2)).toList)
          weight_models(node) = model
        } else if (weight_type == "sublinear7") {
          var weights = world(node).map(keyVal => ((if (subreddit_counts(keyVal) == 0) 1 else scala.math.pow(subreddit_counts(keyVal),0.7)).toDouble+1, keyVal.name)).toList
          var model = new MapResampler(weights)
          weight_models(node) = model
        } else if (weight_type == "sublinear8") {
          var weights = world(node).map(keyVal => ((if (subreddit_counts(keyVal) == 0) 1 else scala.math.pow(subreddit_counts(keyVal),0.8)).toDouble+1, keyVal.name)).toList
          var model = new MapResampler(weights)
          weight_models(node) = model
        } else if (weight_type == "sublinear9") {
          var weights = world(node).map(keyVal => ((if (subreddit_counts(keyVal) == 0) 1 else scala.math.pow(subreddit_counts(keyVal),0.9)).toDouble+1, keyVal.name)).toList
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
  def updateWorld(day: String, world: HashMap[Symbol, ArrayBuffer[Symbol]], self_loop_percent: HashMap[Symbol, Float]):Tuple2[HashMap[Symbol, ArrayBuffer[Symbol]], HashMap[Symbol, Float]] = {
    var world_updates = (parse(new String(Files.readAllBytes(Paths.get("../larger_data/cumulative_daily_nets/"+day)), UTF_8)) \ "subreddit_edges").extract[List[SubredditEdges]]
    for (subreddit <- world_updates) {
      var sub_sym = Symbol(subreddit.subreddit)
      if (!world.contains(sub_sym)){
        world(sub_sym) = ArrayBuffer()
      }
      for (edge <- subreddit.edges){
        var sym_edge = Symbol(edge)
        world(sub_sym) += sym_edge
        if (!self_loop_percent.contains(sym_edge)){
          self_loop_percent(sym_edge) = r.nextFloat
        }
      }
    }
    (world, self_loop_percent)
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
      if (r.nextFloat < self_loop_percent(node)){
        history += node
      } else if (!world.contains(node)){
        history += world.keys.toList(Random.nextInt(world.size))
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
//  var walk_types = List("sqrt", "third_root", "fourth_root")
//  var walk_types = List("sublinear1", "sublinear2", "sublinear3", "sublinear4", "sublinear5", "sublinear6", "sublinear7", "sublinear8", "sublinear9", "sqrt", "third_root", "fourth_root", "log", "proportional", "uniform")
//EVEN OUT THE EFFECT OF 1/k AND SUBLINEAR PREFERENTIAL ATTACHMENT. There is an effect of an edge existing and then a little more effect of the traffic to a subreddit. The edge existence is primary structure, then the sublinear bit drives rich get richer of large subreddits. Without 1/k, early subreddits dominate. Without sublinear pref attachment, movement is too flat to the point that transit counts overestimate small subreddits and underestimate large subreddits.
  var walk_types = List("sublinear5", "sublinear6", "sublinear7", "sublinear8")
  var start_time = System.currentTimeMillis()
  var days = getListOfFiles("../larger_data/user_counts").sorted
  var world = new HashMap[Symbol, ArrayBuffer[Symbol]]//
  var user_counts = new HashMap[Symbol, Int]//
  var last_position = new HashMap[Symbol, Symbol]
  var self_loop_percent = new HashMap[Symbol, Float]//
  var subreddit_counts = new HashMap[Symbol, Int]  { override def default(key:Symbol) = 0 }
  var weight_samples = updateWeightModels("none", world, subreddit_counts)
  1 to 60 foreach {c =>
//    var weight_type = walk_types(c%4)
    var weight_type = "sublinear6"
    for (day <- days.take(2000)){
    // for(day <- days.slice(1190, 1200)){
      var day_str = day.getName
      println(day_str)
      println("Update Self Loops")
      self_loop_percent = self_loop_percent ++ updateSelfLoopPcts(day_str)
      println("Update World")
      var world_res = updateWorld(day_str, world, self_loop_percent)
      world = world_res._1
      self_loop_percent = world_res._2
      println("Update User Counts")
      user_counts = updateUserCounts(day_str)
      println("Update Last Positions for new Users")
      last_position = last_position ++ updateLastPositions(day_str)
      weight_samples = updateWeightModels(weight_type, world, subreddit_counts)
      println("Run Walk")
      var histories = user_counts.par.map(keyVal => walk(keyVal._1, keyVal._2, last_position(keyVal._1), world, self_loop_percent, weight_samples))
      println("Update Stats")
      var stats = update(histories.toList, subreddit_counts)
      println("Update Positions")
      last_position = last_position ++ stats._2
      println("Total Transits:")
      println(subreddit_counts.values.sum)
    }
    // subreddit_counts('GGGGive)
    // var weights = world(node).map(keyVal => ((if (subreddit_counts(keyVal) < 10) 1 else scala.math.pow(subreddit_counts(keyVal),0.6)).toDouble+1, keyVal.name)).toList
    // println(weights)
    // 0, 378, 0, 0
    // (2.9331820449317627,GGGGive)
    Files.write(Paths.get("scala_random_walk_subreddit_counts_"+weight_type+"_3000_days"+r.nextInt(1000000).toString+".txt"), write(subreddit_counts).getBytes(StandardCharsets.UTF_8))
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