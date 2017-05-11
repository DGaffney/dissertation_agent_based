# WORLD is static global hash of "subreddit" => ["outbound_subreddit", ...]
# STATS is a static global hash of "subreddit" => { attribute: value }
# WALKERS is a static global list of { username: "derpderp", starting_sub: "some_subreddit", post_count: 10 }
# HISTORY is a static global hash of "username" => ["subreddit", ...]
# RESULTS is a hash of "username" => ["subreddit", ...]



WALKERS.each do |walker|
  RESULTS[walker[:username]] = run_walk(WORLD, STATS, walker)
end
