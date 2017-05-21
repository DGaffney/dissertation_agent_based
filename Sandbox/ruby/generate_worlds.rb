require 'csv'
require 'json'
class GenerateWorlds
  def self.start_date
    "2005-06-23"
  end

  def self.path
    "/media/dgaff/backup/Code/dissertation_agent_based/Sandbox/larger_data"
  end

  def self.folders
    ["edge_creation", "self_loop_percents", "user_counts", "user_starts"]
  end

  def self.folder
    "worlds"
  end

  def self.run
    world = {}
    `mkdir #{self.path+"/worlds"}`
    self.folders.collect{|f| `ls #{self.path+"/"+f}`.split("\n")}.flatten.uniq.sort.each do |day|
      CSV.read(self.path+"/edge_creation/#{day}").each do |edge|
        world[edge.first]||=[]
        world[edge.first] << edge.last
      end
      f = File.open(self.path+"/worlds/"+day, "w")
      f.write(world.to_json)
      f.close
    end
  end

  def self.run
    world = {}
    subreddits = {}
    cur_int = 0
    `mkdir #{self.path+"/worlds_int"}`
    self.folders.collect{|f| `ls #{self.path+"/"+f}`.split("\n")}.flatten.uniq.sort.each do |day|
      CSV.read(self.path+"/edge_creation/#{day}").each do |edge|
        if subreddits[edge.first].nil?
          cur_int += 1
          subreddits[edge.first] = cur_int
        end
        if subreddits[edge.last].nil?
          cur_int += 1
          subreddits[edge.last] = cur_int
        end
        world[subreddits[edge.first]]||=[]
        world[subreddits[edge.first]] << subreddits[edge.last]
      end
      f = File.open(self.path+"/worlds_int/"+day, "w")
      f.write(world.to_json)
      f.close
    end
  end
end