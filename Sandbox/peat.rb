# WORLD is static global hash of "subreddit" => ["outbound_subreddit", ...]
# STATS is a static global hash of "subreddit" => { attribute: value }
# WALKERS is a static global list of { username: "derpderp", starting_sub: "some_subreddit", post_count: 10 }
# HISTORY is a static global hash of "username" => ["subreddit", ...]
# RESULTS is a hash of "username" => ["subreddit", ...]
require 'csv'
require 'json'
def read_initial_net
  net = JSON.parse(File.read("initial_net.json"))
  (net.keys|net.values.flatten).each do |subreddit|
    net[subreddit] ||= []
  end
  return net
end

def update_walkers(day)
  counts = Hash[CSV.read("user_counts/#{day}")]
  user_starts = Hash[CSV.read("user_starts/#{day}")]
  updated = []
  WALKERS.each do |walker|
    if counts[walker[:username]]
      walker[:transit_count] = counts[walker[:username]].to_i
      updated << walker[:username]
    else
      walker[:transit_count] = 0
    end
  end
  (counts.keys-updated).each do |new_walker|
    WALKERS << {username: new_walker, current_node: user_starts[new_walker], transit_count: counts[new_walker].to_i}
  end
end

def update_net(day)
  new_edges = CSV.read("edge_creation/#{day}")
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
  self_loops = Hash[CSV.read("self_loop_percents/#{day}")]
  WORLD.keys.each do |subreddit, stat_hash|
    STATS[subreddit] ||= {self_loop_percent: rand/10000}
    STATS[subreddit][:self_loop_percent] ||= self_loops[subreddit]||STATS[subreddit][:self_loop_percent]||rand/10000
  end
end
RESULTS = {}
STATS = {}
WORLD = read_initial_net
STATS = initial_stats
days = `ls user_counts`.split("\n")
WALKERS = []
days.each do |day|
  puts day
  RESULTS = {}
  update_walkers(day)
  update_net(day)
  update_stats(day)
  WALKERS.each do |walker|
    RESULTS[walker[:username]] = run_walk(WORLD, STATS, walker)
  end
end