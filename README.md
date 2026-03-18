# uzdist

PySpark distributed unzip on Cloudera CDP cluster

<p align="center">
<img width="384" height="256" alt="uzdist_logo" src="https://github.com/user-attachments/assets/1cbac238-65d7-4994-9f10-139f0e30a518" />
</p>

------------------------------------------------------------------------

## 📦 Requirements

### 🐍 Python

``` bash
$ python3 --version
Python 3.6.8
```

### 📚 Python Libraries

Must be installed across **all cluster nodes**:

-   `pyarrow==6.0.1`

------------------------------------------------------------------------

### ⚡ PySpark

``` bash
$ pyspark3 --version
```

    Welcome to
          ____              __
         / __/__  ___ _____/ /__
        _\ \/ _ \/ _ `/ __/  '_/
       /___/ .__/\_,_/_/ /_/\_\   version 3.3.2.3.3.7191000.9-2
          /_/

    Using Scala version 2.12.15
    OpenJDK 64-Bit Server VM, 11.0.29

    Branch: HEAD
    Compiled on: 2025-04-10T08:16:48Z
    Revision: cde510ecce252d159de8b9a3d6d97e4c49f612c8
    Repository: git@github.infra.cloudera.com:CDH/spark3.git

------------------------------------------------------------------------

## 🧩 Notes

-   Ensure that all dependencies are properly distributed on each node in the cluster.
-   Compatible with CDP (Cloudera Data Platform) environments.
