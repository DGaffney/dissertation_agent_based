Grab the full dataset [here](http://yourdefaulthomepage.com/larger_dataset.zip).

This provides four folders:

* "edge\_creation" has a file for each day of Reddit - each row is a source,target pair for a directed edge that was created on this day in Reddit's history.
* "self\_loop\_percents" has a file for each day of Reddit - each row is a subreddit,percent where the percent denotes the amount of transits users made that wound up being a self loop (i.e. they just stayed at this subreddit) for the given day in Reddit's history.
* "user\_counts" has a file for each day of Reddit - each row is a username,count where the count is the total number of times this user posted on this day in Reddit's history.
* "user\_starts" has a file for each day of Reddit - each row is a username,subreddit where each row represents a new user on Reddit, and the subreddit is where they made their first post.