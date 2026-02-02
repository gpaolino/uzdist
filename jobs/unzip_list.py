from pyspark.sql import SparkSession
import zipfile
import io

spark = SparkSession.builder \
    .appName("APP_ListZipFileContent") \
    .getOrCreate()

sc = spark.sparkContext

zip_hdfs_path = "hdfs://nameservice1/path_to_my_zip_file.zip"


def read_hdfs_file(path: str) -> bytes:
    """
    Reads a binary file from HDFS and returns Python bytes.
    Uses the Java classes exposed by PySpark (sc._jvm), no subprocesses.
    """
    jvm = sc._jvm
    conf = sc._jsc.hadoopConfiguration()

    fs_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = fs_path.getFileSystem(conf)
    stream = fs.open(fs_path)

    """
    Use a Java utility to read the entire stream into a byte[]
    You can use one of these here, depending on what you have in your classpath:
     - org.apache.commons.io.IOUtils
     - org.apache.hadoop.io.IOUtils
    """
    
    data_java = jvm.org.apache.hadoop.io.IOUtils.readFullyToByteArray(stream)
    stream.close()

    # Py4J automatically converts Java bytes[] to Python bytes
    data = bytes(data_java)
    return data


def list_zip_entries(path: str):
    data = read_hdfs_file(path)
    z = zipfile.ZipFile(io.BytesIO(data))
    names = z.namelist()
    z.close()
    return names


entries = list_zip_entries(zip_hdfs_path)

print("ZIP content:")
for name in entries:
    print(name)

spark.stop()
