# WORLD is a hash of "subreddit" => ["outbound_subreddit", ...]
# STATS is a hash of "subreddit" => { attribute: value }
# WALKERS is a list of { username: "derpderp", starting_sub: "some_subreddit", post_count: 10 }
# HISTORY is a hash of "username" => ["subreddit", ...]
# RESULTS is a hash of "username" => ["subreddit", ...]

WALKERS.each do |walker|
  RESULTS[walker[:username]] = run_walk(WORLD, STATS, walker)
end
