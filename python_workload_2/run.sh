export PYSPARK_PYTHON=python3
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    workload_2.py \
    --input movies/ \
    --output workload_2/