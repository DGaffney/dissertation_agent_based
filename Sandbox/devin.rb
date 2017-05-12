def run_walk(world, stats, walker)
  current_node = walker[:current_node]
  transit_count = walker[:transit_count]
  transits = []
  transit_count.times do |t|
    if rand < (stats[current_node] && stats[current_node][:self_loop_percent] || rand/10000) || (world[current_node].nil? || world[current_node].length == 0)
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
