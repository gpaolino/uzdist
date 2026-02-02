from pyspark.sql import SparkSession
import zipfile
from pyarrow import fs

spark = SparkSession.builder \
    .appName("APP_DistributedUnzip") \
    .getOrCreate()

sc = spark.sparkContext

zip_hdfs_path = "/path_to_my_zip_file.zip"
output_dir = "/path_to_my_output_directory"

# ============================================================
# 1. HDFS connection via PyArrow (Python-like files)
# ============================================================

hdfs = fs.HadoopFileSystem("nameservice1")


# ============================================================
# 2. Driver: Read ONLY ZIP index
# ============================================================

def list_zip_entries(path):

    # Open ZIP files as real Python file
    with hdfs.open_input_file(path) as f:
        # Zipfile can also work on file-like objects
        # It doesn't load everything into memory, it only reads ZIP metadata
        z = zipfile.ZipFile(f)

        names = z.namelist()

        z.close()

    return names


# Entries is a Python list with all the internal files.
entries = list_zip_entries(zip_hdfs_path)

# Internal file parallelization, transform list into Spark RDD.
# One task for each internal file, distributed across the executors.
rdd = sc.parallelize(entries, len(entries))


# ============================================================
# 3. Worker: Real streaming extraction
# ============================================================

def extract_streaming(filename):
    # Each executor must import libraries locally.
    import zipfile
    from pyarrow import fs

    hdfs = fs.HadoopFileSystem("nameservice1")

    # Open ZIP
    with hdfs.open_input_file(zip_hdfs_path) as f:
        z = zipfile.ZipFile(f)

        # Internal file stream (unpacked on the fly)
        with z.open(filename) as infile:

            target_path = output_dir + "/" + filename

            # Open HDFS output
            with hdfs.open_output_stream(target_path) as out:

                while True:
                    # Reads up to 1MB in the internal stream.
                    chunk = infile.read(1024 * 1024)  # 1MB
                    # If there is no more data then end of file.
                    if not chunk:
                        break
                    # Writes bytes directly to HDFS.
                    out.write(chunk)

        z.close()

    return filename


# ============================================================
# 4. Run in distributed mode
# ============================================================

results = rdd.map(extract_streaming).collect()

print("Extracted:")
for f in results:
    print(f)

spark.stop()
