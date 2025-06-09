#!/usr/bin/env bash
set -euo pipefail

USER_HDFS="/user/$(whoami)/pagerank_ppr"
STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar"
MAPPER="pagerank_mapper.py"
REDUCER="pagerank_reducer.py"
GRAPH="graph.txt"
EPSILON=1e-6
MAX_ITER=10
NUM_REDUCERS=1

chmod +x $MAPPER $REDUCER

# Bước 1: Dọn & tải dữ liệu
hdfs dfs -rm -r -f $USER_HDFS
hdfs dfs -mkdir -p $USER_HDFS/input $USER_HDFS/iter_0
hdfs dfs -put -f $GRAPH $USER_HDFS/input/
hdfs dfs -put -f $GRAPH $USER_HDFS/
hdfs dfs -cp $USER_HDFS/input/$GRAPH $USER_HDFS/iter_0/part-00000

# Bước 2: Lặp tính PageRank
for ((i=1; i<=MAX_ITER; i++)); do
  IN="$USER_HDFS/iter_$((i-1))"
  OUT="$USER_HDFS/iter_$i"
  echo "=== Iteration $i ==="
  hdfs dfs -rm -r -f $OUT

  hadoop jar $STREAMING_JAR \
    -D mapreduce.job.reduces=$NUM_REDUCERS \
    -files $MAPPER,$REDUCER,$GRAPH \
    -mapper "python3 $MAPPER" \
    -reducer "python3 $REDUCER" \
    -input $IN \
    -output $OUT

  # Kiểm tra hội tụ
  PREV_SORTED=$(mktemp)
  CURR_SORTED=$(mktemp)
  hdfs dfs -cat $IN/part-* | sort -k1,1 > $PREV_SORTED
  hdfs dfs -cat $OUT/part-* | sort -k1,1 > $CURR_SORTED

  DELTA=$(paste $PREV_SORTED $CURR_SORTED | awk '{d=$2-$4; if(d<0)d=-d; print d}' | sort -nr | head -1)
  echo "Max delta = $DELTA"
  rm -f $PREV_SORTED $CURR_SORTED

  if awk "BEGIN{exit !($DELTA < $EPSILON)}"; then
    echo "✅ Converged at iteration $i"
    LAST_ITER=$i
    break
  fi

  LAST_ITER=$i
done

# Bước 3: Gom kết quả
hdfs dfs -rm -r -f $USER_HDFS/all_iters
hdfs dfs -mkdir -p $USER_HDFS/all_iters
hdfs dfs -cp $USER_HDFS/iter_$LAST_ITER/part-* $USER_HDFS/all_iters/

# Bước 4: Xuất kết quả ranking
hdfs dfs -cat $USER_HDFS/all_iters/* > /tmp/ppr_final.tsv

awk -F $'\t' -v src="P1" '$1!=src { print $1, $2 }' /tmp/ppr_final.tsv \
  | sort -k2,2nr | awk '{printf("%d\t%s\t%f\n", NR, $1, $2)}' > ~/ppr.txt

echo "Ranking written to ~/ppr.txt"
cat ~/ppr.txt
