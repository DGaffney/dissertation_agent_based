class Timer

  def initialize(name, padding = 0)
    @timers = {}
    @total = 0
    @padding = "\t" * padding
    @name = name
  end

  def time(key)
    if SUPPRESS_TIMER
      yield
      return
    end

    start_time = Time.now
    yield
    end_time = Time.now
    elapsed = (end_time - start_time) * 1000.0
    @total += elapsed

    if @timers[key]
      @timers[key] += elapsed
    else
      @timers[key] = elapsed
    end
  end

  def pct(key)
    return if SUPPRESS_TIMER

    return 0 if @total <= 0
    @timers[key].to_f / @total.to_f
  end

  def summary
    return if SUPPRESS_TIMER

    @timers.each_pair do |key, ms|
      puts "#{@padding}#{key}: #{ms.to_i}ms, #{(pct(key) * 100).to_i}%"
    end
    puts "#{@padding}#{@name} TOTAL: #{@total.to_i}ms"
  end

end
