files = `ls /media/dgaff/backup/Code/reddit_random_walk/code/results/dataset_full/data/baumgartner_daily_non_self_loops`.split("\n").select{|x| x.split("-").length == 3}.sort[0..1999]
counts = Hash.new(0)
require 'csv'
files.each do |file|
  Hash[CSV.read("/media/dgaff/backup/Code/reddit_random_walk/code/results/dataset_full/data/baumgartner_daily_non_self_loops/#{file}")].each do |k,v|
    counts[k] ||= 0
    counts[k] += v.to_i
  end
  puts file
end;false
f = File.open("/media/dgaff/backup/Code/dissertation_agent_based/Sandbox/scala/non_self_loops_cumulative.csv", "w")
f.write(counts.to_a.collect{|x| x.join(",")}.join("\n"))
f.close
files = `ls /media/dgaff/backup/Code/reddit_random_walk/code/results/dataset_full/data/baumgartner_daily_self_loops`.split("\n").select{|x| x.split("-").length == 3}.sort[0..1999]
self_loop_counts = Hash.new(0)
require 'csv'
files.each do |file|
  Hash[CSV.read("/media/dgaff/backup/Code/reddit_random_walk/code/results/dataset_full/data/baumgartner_daily_self_loops/#{file}")].each do |k,v|
    self_loop_counts[k] ||= 0
    self_loop_counts[k] += v.to_i
  end
  puts file
end;false
f = File.open("/media/dgaff/backup/Code/dissertation_agent_based/Sandbox/scala/self_loops_cumulative.csv", "w")
f.write(counts.to_a.collect{|x| x.join(",")}.join("\n"))
f.close
full_counts = Hash[(counts.keys|self_loop_counts.keys).collect{|k| [k, counts[k]||0 + self_loop_counts[k]||0]}];false
f = File.open("/media/dgaff/backup/Code/dissertation_agent_based/Sandbox/scala/cumulative.csv", "w")
f.write(full_counts.to_a.collect{|x| x.join(",")}.join("\n"))
f.close

subreddit_counts = {}
`ls | grep scala_random_walk_subreddit_counts`.split("\n").collect{|file| 
  run_type = file.split("_")[5]; 
  subreddit_counts[run_type]||={}
  data = JSON.parse(File.read(file))
  data.each do |k,v|
    subreddit_counts[run_type][k]||= []
    subreddit_counts[run_type][k] << v.to_i
  end
};false
subreddit_counts["observed"] = Hash[CSV.read("first_2000_days/cumulative.csv").collect{|k,v|[k,v.to_i]}];false
load '/media/dgaff/backup/Code/reddit_random_walk/code/extensions/array.rb'
csv = CSV.open("first_2000_days_output_skip_23.csv", "w")
csv << [
  "Subreddit", 
  "Observed", 
  "Uniform Model", 
  "Log Pref Model", 
  "Proportional Pref Model", 
  "Sqrt Model",
  "Third Root Model",
  "Fourth Root Model",
  "Sublinear k^0.1",
  "Sublinear k^0.2",
  "Sublinear k^0.3",
  "Sublinear k^0.4",
  "Sublinear k^0.5",
  "Sublinear k^0.6",
  "Sublinear k^0.7",
  "Sublinear k^0.8",
  "Sublinear k^0.9",
  "Log(Observed)", 
  "Log(Uniform Model)", 
  "Log(Log Pref Model)", 
  "Log(Proportional Pref Model)",
  "Log(Sqrt Model)",
  "Log(Third Root Model)",
  "Log(Fourth Root Model)",
  "Log(Sublinear k^0.1)",
  "Log(Sublinear k^0.2)",
  "Log(Sublinear k^0.3)",
  "Log(Sublinear k^0.4)",
  "Log(Sublinear k^0.5)",
  "Log(Sublinear k^0.6)",
  "Log(Sublinear k^0.7)",
  "Log(Sublinear k^0.8)",
  "Log(Sublinear k^0.9)"]
subreddit_counts["observed"].sort_by{|k,v| v}.reverse.each_slice(23).collect(&:first).each do |k, v|
  uniform_val = subreddit_counts["uniform"][k].median rescue 0
  log_val = subreddit_counts["log"][k].median rescue 0
  prop_val = subreddit_counts["proportional"][k].median rescue 0
  sqrt = subreddit_counts["sqrt"][k].median rescue 0
  third = subreddit_counts["third"][k].median rescue 0
  fourth = subreddit_counts["fourth"][k].median rescue 0
  sublinear1 = subreddit_counts["sublinear1"][k].median rescue 0
  sublinear2 = subreddit_counts["sublinear2"][k].median rescue 0
  sublinear3 = subreddit_counts["sublinear3"][k].median rescue 0
  sublinear4 = subreddit_counts["sublinear4"][k].median rescue 0
  sublinear5 = subreddit_counts["sublinear5"][k].median rescue 0
  sublinear6 = subreddit_counts["sublinear6"][k].median rescue 0
  sublinear7 = subreddit_counts["sublinear7"][k].median rescue 0
  sublinear8 = subreddit_counts["sublinear8"][k].median rescue 0
  sublinear9 = subreddit_counts["sublinear9"][k].median rescue 0
  counts = {
    uniform: uniform_val, 
    log: log_val, 
    proportional: prop_val, 
    sqrt: sqrt, 
    third: third, 
    fourth: fourth,
    sublinear1: sublinear1,
    sublinear2: sublinear2,
    sublinear3: sublinear3,
    sublinear4: sublinear4,
    sublinear5: sublinear5,
    sublinear6: sublinear6,
    sublinear7: sublinear7,
    sublinear8: sublinear8,
    sublinear9: sublinear9
  }
  csv << [
    k, 
    v, 
    counts[:uniform], 
    counts[:log], 
    counts[:proportional], 
    counts[:sqrt], 
    counts[:third], 
    counts[:fourth], 
    counts[:sublinear1],
    counts[:sublinear2],
    counts[:sublinear3],
    counts[:sublinear4],
    counts[:sublinear5],
    counts[:sublinear6],
    counts[:sublinear7],
    counts[:sublinear8],
    counts[:sublinear9],
    v == 0 ? 0 : Math.log10(v), 
    counts[:uniform] == 0 ? 0 : Math.log10(counts[:uniform]), 
    counts[:log] == 0 ? 0 : Math.log10(counts[:log]), 
    counts[:proportional] == 0 ? 0 : Math.log10(counts[:proportional]), 
    counts[:sqrt] == 0 ? 0 : Math.log10(counts[:sqrt]), 
    counts[:third] == 0 ? 0 : Math.log10(counts[:third]), 
    counts[:fourth] == 0 ? 0 : Math.log10(counts[:fourth]),
    counts[:sublinear1] == 0 ? 0 : Math.log10(counts[:sublinear1]),
    counts[:sublinear2] == 0 ? 0 : Math.log10(counts[:sublinear2]),
    counts[:sublinear3] == 0 ? 0 : Math.log10(counts[:sublinear3]),
    counts[:sublinear4] == 0 ? 0 : Math.log10(counts[:sublinear4]),
    counts[:sublinear5] == 0 ? 0 : Math.log10(counts[:sublinear5]),
    counts[:sublinear6] == 0 ? 0 : Math.log10(counts[:sublinear6]),
    counts[:sublinear7] == 0 ? 0 : Math.log10(counts[:sublinear7]),
    counts[:sublinear8] == 0 ? 0 : Math.log10(counts[:sublinear8]),
    counts[:sublinear9] == 0 ? 0 : Math.log10(counts[:sublinear9])
    ]
end;false
csv.close
csv = CSV.read("first_2000_days_output_skip_23.csv")
new_csv = CSV.open("first_2000_days_sorted_dists.csv", "w")
new_csv << csv.first[1..16]
csv.transpose[1..16].collect{|x| x[1..-1].collect(&:to_f).sort.reverse}.transpose.each do |row|
new_csv << row
end;false
new_csv.close

