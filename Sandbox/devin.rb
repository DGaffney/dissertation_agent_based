def run_walk(world, stats, username, current_node, transit_count)
  transits = []
  transit_count.to_i.times do |t|
    if rand < stats["self_loops"][current_node]
      transits << current_node
    else
      transits << world[current_node].shuffle.first
    end
    current_node = transits.last
  end
  return transits
end
