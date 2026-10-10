import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import time
import re
from pyspark.sql import SparkSession
# ... (lanjutkan dengan kode yang sebelumnya ada di bawahnya) ...
# 1. Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("WordCount_Analysis") \
    .master("local[*]") \
    .getOrCreate()
sc = spark.sparkContext

# 2. Fungsi Preprocessing (Membersihkan tanda baca dan huruf kecil)
def clean_text(text):
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text

print("=== Top 10 Kata Terbanyak ===")
# Membaca data yang tadi kita buat
raw_data = sc.textFile("dataset_5000.txt")

# Transformasi Data
cleaned_data = raw_data.map(clean_text)
words = cleaned_data.flatMap(lambda line: line.split())
word_tuples = words.map(lambda word: (word, 1))
word_counts = word_tuples.reduceByKey(lambda a, b: a + b)
sorted_counts = word_counts.sortBy(lambda x: x[1], ascending=False)

# Action
top_10 = sorted_counts.take(10)
for word, count in top_10:
    print(f"{word}: {count}")

print("\n=== Analisis Performa Jumlah Partition ===")
partitions_to_test = [1, 2, 4, 8, 16]

for p in partitions_to_test:
    start_time = time.time()
    
    test_rdd = raw_data.repartition(p)
    result = test_rdd.map(clean_text) \
                     .flatMap(lambda line: line.split()) \
                     .map(lambda word: (word, 1)) \
                     .reduceByKey(lambda a, b: a + b) \
                     .sortBy(lambda x: x[1], ascending=False) \
                     .take(10)
                     
    end_time = time.time()
    print(f"Jumlah Partition: {p:2d} | Waktu Eksekusi: {end_time - start_time:.4f} detik")

# Menutup program
spark.stop()