def run_walk(world, stats, username, current_node, transit_count)
  transits = []
  transit_count.to_i.times do |t|
    if rand < stats["self_loops"][current_node]
      transits << current_node
    else
      # for the love of Christ, don't shuffle; it reorders in memory. Far less expensive to pick a random index.
      neighbors = world[current_node]
      transits << neighbors[rand(neighbors.length)]
    end
    current_node = transits.last
  end
  return transits
end
