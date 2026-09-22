# Hands-on L6: Report

**Name: Hardi Koradiya**
**Student ID: 801484363**
**Email:hkoradiy@charlotte.edu**

---

## Seed and commands

Seed used for `datagen.py`: 801484363

The assignment was completed using the provided Docker/Spark setup and the supplied template. I implemented the TODO sections in `main.py`, generated the input data, submitted the Spark application with `spark-submit`, inspected the generated CSV results, and inspected the Spark UI and physical execution plan.

The main commands used were:

```bash
# Generate the input data
python3 datagen.py YOUR_SEED

# Start the Docker Spark cluster
docker compose up -d

# Copy/run the assignment through Spark
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output

# Inspect the generated output
find shared-folder/output -maxdepth 2 -type f | sort

# Display the results
cat shared-folder/output/task1/part-*.csv
cat shared-folder/output/task2/part-*.csv
cat shared-folder/output/task3/part-*.csv
cat shared-folder/output/task4/part-*.csv

```bash

```

---

## Results

For each task, the first ten rows of your output (from the terminal or the CSV file) and one
or two sentences on what they say about your data.

### Task 1: favorite genre per user

```
The task counts plays for every (user_id, genre) combination and then uses row_number() to select the genre with the highest play count for each user. When there is a tie, the genre is ordered alphabetically as required.

9/22 23:00:32 INFO DAGScheduler: Asked to cleanup jobs for query execution 2
+--------+---------+----------+
|user_id |genre    |play_count|
+--------+---------+----------+
|user_1  |Jazz     |9         |
|user_10 |Jazz     |6         |
|user_100|Hip-Hop  |7         |
|user_11 |Rock     |2         |
|user_12 |Pop      |6         |
|user_13 |Jazz     |7         |
|user_14 |Jazz     |7         |
|user_15 |Classical|5         |
|user_16 |Rock     |9         |
|user_17 |Hip-Hop  |8         |
|user_18 |Jazz     |4         |
|user_19 |Jazz     |9         |
|user_2  |Rock     |9         |
|user_20 |Rock     |11        |
|user_21 |Rock     |6         |
|user_22 |Pop      |7         |
|user_23 |Jazz     |5         |
|user_24 |Classical|6         |
|user_25 |Pop      |7         |
|user_26 |Pop      |7         |
+--------+---------+----------+
only showing top 20 rows

The results show that users have different dominant genres. For example, user_1 played Jazz 9 times, while user_15 played Classical 5 times.
```

### Task 2: average listening time per song

```
This task calculates the average duration_sec for each song and counts how many times each song was played. The results are rounded to two decimal places and sorted from the longest average listening time to the shortest.

26/09/22 23:03:15 INFO DAGScheduler: Asked to cleanup jobs for query execution 4
+-------+-------------+----------------+----------+
|song_id|title        |avg_duration_sec|play_count|
+-------+-------------+----------------+----------+
|song_23|Title_song_23|199.22          |9         |
|song_6 |Title_song_6 |186.93          |41        |
|song_34|Title_song_34|185.27          |15        |
|song_49|Title_song_49|183.75          |12        |
|song_45|Title_song_45|183.22          |9         |
|song_20|Title_song_20|182.84          |25        |
|song_46|Title_song_46|181.88          |16        |
|song_39|Title_song_39|181.45          |11        |
|song_25|Title_song_25|179.13          |15        |
|song_10|Title_song_10|178.92          |12        |
|song_30|Title_song_30|178.28          |40        |
|song_35|Title_song_35|177.27          |15        |
|song_36|Title_song_36|177.26          |19        |
|song_29|Title_song_29|176.27          |15        |
|song_21|Title_song_21|175.83          |18        |
|song_12|Title_song_12|175.44          |16        |
|song_32|Title_song_32|174.19          |43        |
|song_44|Title_song_44|174.06          |18        |
|song_15|Title_song_15|172.19          |16        |
|song_16|Title_song_16|169.17          |18        |
+-------+-------------+----------------+----------+
only showing top 20 rows

song_23 has the highest average listening duration in the displayed results at 199.22 seconds. The play count varies considerably between songs, so average duration and popularity are not necessarily the same thing.
```

### Task 3: genre loyalty score, top 10

```
The loyalty score is calculated as the number of plays in the user's favorite genre divided by the user's total number of plays. The result is rounded to three decimal places and the ten highest scores are returned.

26/09/22 23:05:18 INFO DAGScheduler: Asked to cleanup jobs for query execution 6
+-------+---------+----------+-----------+-------------+
|user_id|genre    |play_count|total_plays|loyalty_score|
+-------+---------+----------+-----------+-------------+
|user_80|Rock     |9         |9          |1.0          |
|user_51|Classical|8         |8          |1.0          |
|user_13|Jazz     |7         |7          |1.0          |
|user_88|Classical|6         |6          |1.0          |
|user_87|Hip-Hop  |5         |5          |1.0          |
|user_20|Rock     |11        |12         |0.917        |
|user_96|Pop      |11        |12         |0.917        |
|user_65|Hip-Hop  |10        |11         |0.909        |
|user_19|Jazz     |9         |10         |0.9          |
|user_17|Hip-Hop  |8         |9          |0.889        |
+-------+---------+----------+-----------+-------------+

26/09/22 23:05:18 INFO FileSourceStrategy: Pushed Filters: IsNotNull(song_id),IsNotNull(user_id)

Users with relatively few plays can receive a loyalty score of 1.0 because every play in their sample belongs to their favorite genre. For example, user_88 has 6 total plays and all 6 are Classical, producing a score of 1.0.

A score of 1.0 does not necessarily mean that the user has demonstrated strong long-term loyalty because the sample size may be small. One possible modification would be to require a minimum number of plays before reporting a loyalty score, or to combine the proportion with a minimum-play threshold.
```

Why do users with few plays tend to get a score of 1.0? Would you change the definition of
the score to account for that?

### Task 4: night owls

```
This task identifies plays occurring from 12:00 AM through 4:59 AM by using hour("timestamp") and filtering for hours 0 through 4. The results are grouped by user and sorted by the number of night plays.

26/09/22 23:06:40 INFO DAGScheduler: Asked to cleanup jobs for query execution 8
+-------+-----------+
|user_id|night_plays|
+-------+-----------+
|user_2 |6          |
|user_82|6          |
|user_22|5          |
|user_39|5          |
|user_40|5          |
|user_44|5          |
|user_51|5          |
|user_70|5          |
|user_16|4          |
|user_27|4          |
|user_52|4          |
|user_6 |4          |
|user_61|4          |
|user_8 |4          |
|user_89|4          |
|user_95|4          |
|user_18|3          |
|user_25|3          |
|user_26|3          |
|user_32|3          |
+-------+-----------+
only showing top 20 rows

user_2 and user_82 have the highest number of night-time plays in the results, with 6 each. The output demonstrates that the timestamp was successfully parsed as a real timestamp because the Spark hour() function can be applied directly to it.
```

---

## The plan

Paste the `explain()` output of task 1:

```
26/09/22 23:00:33 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(song_id#4))
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [user_id#0 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(user_id#0 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=975]
      +- Project [user_id#0, genre#7, play_count#28L]
         +- Filter (rank#38 = 1)
            +- Window [row_number() windowspecdefinition(user_id#0, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#38], [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST]
               +- WindowGroupLimit [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], row_number(), 1, Final
                  +- Sort [user_id#0 ASC NULLS FIRST, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], false, 0
                     +- Exchange hashpartitioning(user_id#0, 200), ENSURE_REQUIREMENTS, [plan_id=968]
                        +- WindowGroupLimit [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], row_number(), 1, Partial
                           +- Sort [user_id#0 ASC NULLS FIRST, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], false, 0
                              +- HashAggregate(keys=[user_id#0, genre#7], functions=[count(1)])
                                 +- Exchange hashpartitioning(user_id#0, genre#7, 200), ENSURE_REQUIREMENTS, [plan_id=962]
                                    +- HashAggregate(keys=[user_id#0, genre#7], functions=[partial_count(1)])
                                       +- Project [user_id#0, genre#7]
                                          +- BroadcastHashJoin [song_id#1], [song_id#4], Inner, BuildRight, false, false
                                             :- Filter isnotnull(song_id#1)
                                             :  +- FileScan csv [user_id#0,song_id#1] Batched: false, DataFilters: [isnotnull(song_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/listening_logs.csv], PartitionFilters: [], PushedFilters: [IsNotNull(song_id)], ReadSchema: struct<user_id:string,song_id:string>
                                             +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=957]
                                                +- Filter isnotnull(song_id#4)
                                                   +- FileScan csv [song_id#4,genre#7] Batched: false, DataFilters: [isnotnull(song_id#4)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/songs_metadata.csv], PartitionFilters: [], PushedFilters: [IsNotNull(song_id)], ReadSchema: struct<song_id:string,genre:string>



=== task2: not implemented yet ===

=== task3: not implemented yet ===

=== task4: not implementeed yet ===

Reading of the physical plan

The plan should contain two CSV file scans:

listening_logs.csv, which provides the user and song information needed for the task.

songs_metadata.csv, which provides the song genre information.

The join is shown as a BroadcastHashJoin. Spark chooses the broadcast side for the smaller songs metadata relation, allowing the metadata to be broadcast to the workers instead of shuffling both relations for the join.

The first major shuffle is an Exchange using hash partitioning by user_id and genre. This is needed to bring rows with the same user and genre together for the groupBy("user_id", "genre") aggregation.

Another Exchange uses hash partitioning by user_id. This is needed for the window operation because the row_number() calculation is partitioned by user.

The window orders each user's genres by play_count descending and then genre ascending. The row_number() value is then filtered to rank = 1, leaving one favorite genre per user.

Finally, the orderBy("user_id") causes the final range-partitioning and sort shown near the top of the physical plan.

The Spark UI SQL/DataFrame plan should correspond to these same major operations: file scans, the broadcast join, aggregation, exchanges/shuffles, the window operation, filtering, and final sorting.

```

Your reading of it: where are the two file scans, which operator is the join and which kind
of join did Spark choose, where are the shuffles (`Exchange`) and why are they needed, and
how does this match the diagram in the SQL / DataFrame tab of the Spark UI?



---

## Transformations and actions

Which lines of your `main.py` are actions? How many jobs did the program launch according to
the Spark UI, and is that what you expected?

Transformations in main.py include operations such as:

join()

groupBy()

agg()

withColumn()

filter()

select()

orderBy()

limit()

These operations are lazy transformations: they build the Spark execution plan but do not immediately execute the computation.

The actions in main.py are the operations that trigger execution, including:

logs.count()

songs.count()

df.show(20) inside save()

df.coalesce(1).write...csv(...) inside save()

The favorite.explain() call displays the execution plan and is not a normal data-computation action like show() or write().

### Spark UI job count

Jobs shown in the Spark UI: The captured Jobs page did not display the completed job table, so the screenshots do not provide a reliable final job count.

The captured Stages page showed the application while it was still running. At that time, the UI showed 1 active stage, 1 pending stage, and 1 completed stage (Stage 0). The completed stage had 1/1 task, a duration of about 0.7 seconds, 39.5 KiB of input, and 59.0 B of shuffle write.

The program contains multiple actions that trigger Spark execution: `logs.count()`, `songs.count()`, the four `show()` calls inside `save()`, and the four CSV write operations inside `save()`. Therefore, multiple Spark jobs are expected. Because the captured Jobs page did not show the completed job list, I cannot determine the final number of jobs from the screenshot alone.

The Spark UI Stages page showed 1 active stage, 1 pending stage, and 1 completed stage while the application was running. Stage 0 had 1/1 completed task, approximately 0.7 seconds duration, 39.5 KiB input, and 59.0 B shuffle write. The Jobs page screenshot did not show the completed job table, so it does not establish the final total number of jobs.

---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.

No fatal Spark or Python errors occurred during the final execution of the completed assignment.

The main implementation work was filling the TODO sections in main.py:

defining the explicit StructType schema for listening_logs.csv;
implementing Task 1 with groupBy, count, Window, and row_number;
implementing Task 2 with avg, count, rounding, joining, and sorting;
implementing Task 3 with a total-play aggregation, join, loyalty calculation, sorting, and limit(10);
implementing Task 4 with timestamp-based hour filtering, grouping, counting, and sorting.

The final execution successfully generated output directories for all four tasks:

shared-folder/output/task1/
shared-folder/output/task2/
shared-folder/output/task3/
shared-folder/output/task4/

The generated results were then verified from the CSV files, and the Task 1 physical plan was captured using favorite.explain().