# WORLD is static global hash of "subreddit" => ["outbound_subreddit", ...]
# STATS is a static global hash of "subreddit" => { attribute: value }
# LAST_VISIT is a static global hash of "username" => "subreddit"

require 'csv'
require 'json'

require './timer'

# this will turn off the Timer classes so that they don't log or report
# on the inner workings of the horrible machinery
SUPPRESS_TIMER = true

def read_initial_net
  net = JSON.parse(File.read("../data/initial_net.json"))
  (net.keys|net.values.flatten).each do |subreddit|
    net[subreddit] ||= []
  end
  return net
end

def create_walkers(day)
  timer = Timer.new("Create Walkers", 2)
  counts = nil
  user_starts = nil
  todays_walkers = []

  timer.time(:loading_files) {
    counts = Hash[CSV.read("../data/user_counts/#{day}")]
    user_starts = Hash[CSV.read("../data/user_starts/#{day}")]
  }

  timer.time(:building_walkers) {
    counts.each_pair do |username, count|
      walker = { username: username, transit_count: count.to_i }
      if LAST_VISIT[username]
        walker[:current_node] = LAST_VISIT[username]
      else
        walker[:current_node] = user_starts[username]
      end
      todays_walkers << walker
    end
  }

  timer.summary()

  return todays_walkers
end

def update_last_visit(walker, history)
  LAST_VISIT[walker[:username]] = history.last
end

def update_net(day)
  new_edges = CSV.read("../data/edge_creation/#{day}")
  all_subs = new_edges.flatten.uniq
  new_edges.each do |edge|
    if edge.first != edge.last
      WORLD[edge.first] ||= []
      WORLD[edge.first] << edge.last
    end
  end
  all_subs.each do |subreddit|
    WORLD[subreddit] ||= []
  end
end

def initial_stats
  WORLD.each do |subreddit, edges|
    STATS[subreddit] ||= {self_loop_percent: rand/10000}
  end
  return STATS
end

def update_stats(day)
  timer = Timer.new("Update Stats", 2)
  self_loops = nil

  timer.time(:loading_files) {
    self_loops = Hash[CSV.read("../data/self_loop_percents/#{day}")]
  }

  timer.time(:updating_stats) {
    WORLD.keys.each do |subreddit, stat_hash|
      STATS[subreddit] ||= {self_loop_percent: rand/10000}
      STATS[subreddit][:self_loop_percent] ||= self_loops[subreddit]||STATS[subreddit][:self_loop_percent]||rand/10000
    end
  }

  # negligible time; don't polute the output here.
  # timer.summary()
end

def run_walk(world, stats, walker)
  current_node = walker[:current_node]
  transit_count = walker[:transit_count]
  transits = []

  transit_count.times do |t|
    if rand < (stats[current_node] && stats[current_node][:self_loop_percent] || rand/10000) || (world[current_node].nil? || world[current_node].length == 0)
      transits << current_node
    else
      neighbors = world[current_node]
      transits << neighbors[rand(neighbors.length)]
    end
  end

  return transits
end



STATS = {}

WORLD = read_initial_net
STATS = initial_stats
LAST_VISIT = {}
total_transits = 0

days = `ls ../data/user_counts`.split("\n")

def total_outbound
  sum = 0
  WORLD.each_value do |outs|
    sum += outs.length
  end
  sum
end


simulation_start = Time.now
days.each do |day|
  day_timer = Timer.new("Day", 1)
  puts "-----"
  puts day
  puts "Cumulative data:"
  puts "  Members: #{LAST_VISIT.length}"
  puts "  Subreddits: #{WORLD.length}"
  puts "  Edges: #{total_outbound}"
  puts "  Transits: #{total_transits}"
  puts "-----"

  todays_walkers = []

  day_timer.time(:update_walkers) {
    todays_walkers = create_walkers(day)
  }

  day_timer.time(:update_net) {
    update_net(day)
  }

  day_timer.time(:update_stats) {
    update_stats(day)
  }

  day_timer.time(:walkers) {
    todays_walkers.each do |walker|
      history = run_walk(WORLD, STATS, walker)
      update_last_visit(walker, history)
      total_transits += history.length
    end
  }

  day_timer.summary()
end
elapsed = Time.now - simulation_start
puts "Elapsed: #{elapsed} seconds"
puts "Transits/Second: #{(total_transits / elapsed).to_i}"
