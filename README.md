# ETL with Airflow
---

##  Objectives

Mengevaluasi konsep pembelajaran sebagai berikut:

- Menggunakan Apache Airflow
- Melakukan validasi data dengan menggunakan Great Expectations
- Mempersiapkan data untuk digunakan sebelum masuk ke database NoSQL.
- Mengolah dan memvisualisasikan data dengan menggunakan Kibana.

## Dataset
Dataset diperoleh dari kaggle pada link berikut:
https://www.kaggle.com/datasets/lainguyn123/australia-car-market-data

## Problems
Exploratory Data Analysis (EDA) akan dihasilkan dari dataset dengan terlebih dahulu ditentukan mengenai objective yang hendak dicapai. Report akan berisi hasil EDA dan rekomendasi lanjutan terkait dengan objective.  
  
Dataset akan terlebih dahulu dilakukan Data Cleaning dan validasi data menggunakan Great Expectation. Semua proses dilakukan dengan pipeline yang dijalankan menggunakan Apache Airflow. 

## Data Validation
Validasi data dilakukan dengan menggunakan Great Expectations. 

## DAG
Automasi dilakukan dengan membuat DAG dengan kriteria :
   - DAG berisi 3 node/task dibawah ini :
     + `Fetch from Postgresql` : berisi script untuk mengambil data dari PostgreSQL.
     + `Data Cleaning` : berisi script untuk melakukan Data Cleaning dan penyimpanan ke CSV file.
     + `Post to Elasticsearch` : berisi script untuk me-load CSV yang berisi data yang sudah clean dan memasukkannya ke Elasticsearch.
   - Penjadwalan dilakukan setiap jam 06:30.

## Result
Hasil visualisasi dengan kibana terdapat di folder image, beserta capture running DAG