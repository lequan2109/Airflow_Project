from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, input_file_name, regexp_extract, create_map, current_date, to_date, floor, date_add
)
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pandas as pd
import os, sys

spark = (
    SparkSession.builder
    .appName("StockETL_FixedDate")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

print("🚀 Bắt đầu xử lý dữ liệu...")

input_path = "/opt/airflow/data/input/*.csv"
output_dir = "/opt/airflow/data/output"
output_file = f"{output_dir}/cleaned_stock.csv"

# --- Kiểm tra thư mục ---
if not os.path.exists("/opt/airflow/data/input"):
    print("❌ Thư mục input không tồn tại.")
    sys.exit(1)

df = spark.read.option("header", True).csv(input_path)
if df.rdd.isEmpty():
    print("⚠️ Không có file CSV nào trong thư mục input.")
    sys.exit(1)

df = df.withColumn("filename", input_file_name())
df = df.withColumn("symbol", regexp_extract(col("filename"), r"([^/]+)\.csv$", 1))
df = df.toDF(*[c.lower() for c in df.columns])

# --- Xử lý cột date an toàn ---
if "date" not in df.columns:
    df = df.withColumn("date", current_date())
else:
    df = df.withColumn(
        "date",
        when(
            col("date").cast("string").rlike("^[0-9.]+$"),
            date_add(lit("1899-12-30"), floor(col("date").cast("double")).cast("int"))
        )
        .when(
            col("date").rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
            to_date(col("date"), "yyyy-MM-dd")
        )
        .otherwise(to_date(col("date"), "dd/MM/yyyy"))
    )

# --- Ép kiểu dữ liệu ---
df = (
    df.withColumn("open", F.expr("try_cast(open as float)"))
      .withColumn("high", F.expr("try_cast(high as float)"))
      .withColumn("low", F.expr("try_cast(low as float)"))
      .withColumn("close", F.expr("try_cast(close as float)"))
      .withColumn("volume", F.expr("try_cast(volume as bigint)"))
)

df = df.filter(col("close").isNotNull())

# --- Ánh xạ ngành ---
industry_map = {
    "VCB": "Banking", "TCB": "Banking", "CTG": "Banking", "MBB": "Banking", "BID": "Banking",
    "FPT": "Technology", "HPG": "Steel", "HSG": "Steel",
    "MWG": "Retail", "PNJ": "Retail",
    "VHM": "Real Estate", "VIC": "Real Estate", "NVL": "Real Estate",
    "VNM": "Consumer Goods", "SAB": "Consumer Goods", "MSN": "Consumer Goods",
    "GAS": "Oil & Gas", "PLX": "Oil & Gas",
    "SSI": "Securities", "VND": "Securities", "HCM": "Securities"
}
industry_expr = create_map([lit(x) for kv in industry_map.items() for x in kv])
df = df.withColumn("industry", industry_expr[col("symbol")])

# --- Hàm Pandas UDF ---
schema = T.StructType([
    T.StructField("date", T.DateType()),
    T.StructField("open", T.FloatType()),
    T.StructField("high", T.FloatType()),
    T.StructField("low", T.FloatType()),
    T.StructField("close", T.FloatType()),
    T.StructField("volume", T.LongType()),
    T.StructField("symbol", T.StringType()),
    T.StructField("industry", T.StringType()),
    T.StructField("daily_return", T.FloatType()),
    T.StructField("price_range", T.FloatType()),
    T.StructField("volume_sma_5", T.FloatType()),
    T.StructField("sma_5", T.FloatType()),
    T.StructField("sma_20", T.FloatType()),
    T.StructField("ema_12", T.FloatType()),
    T.StructField("ema_26", T.FloatType()),
    T.StructField("macd", T.FloatType()),
    T.StructField("rsi_14", T.FloatType()),
])

def calculate_indicators(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values(by="date")
    pdf["daily_return"] = ((pdf["close"] - pdf["open"]) / pdf["open"]) * 100
    pdf["price_range"] = ((pdf["high"] - pdf["low"]) / pdf["low"]) * 100
    pdf["sma_5"] = pdf["close"].rolling(5).mean()
    pdf["sma_20"] = pdf["close"].rolling(20).mean()
    pdf["volume_sma_5"] = pdf["volume"].rolling(5).mean()
    pdf["ema_12"] = pdf["close"].ewm(span=12, adjust=False).mean()
    pdf["ema_26"] = pdf["close"].ewm(span=26, adjust=False).mean()
    pdf["macd"] = pdf["ema_12"] - pdf["ema_26"]
    delta = pdf["close"].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    pdf["rsi_14"] = 100 - (100 / (1 + rs))
    pdf = pdf.round(2)

    # ===== SỬA LỖI: Chỉ trả về các cột có trong schema =====
    # Lấy danh sách tên cột từ biến schema
    output_columns = [field.name for field in schema.fields]
    
    # Trả về DataFrame chỉ chứa các cột này
    return pdf[output_columns]
    # ======================================================

df_final = df.groupBy("symbol").applyInPandas(calculate_indicators, schema=schema).fillna(0)

# --- Ghi file ---
os.makedirs(output_dir, exist_ok=True)
print(f"🔄 Ghi dữ liệu ra {output_file} ...")
df_final.toPandas().sort_values(by=["symbol", "date"]).to_csv(output_file, index=False)
print(f"✅ File đã lưu: {output_file}")

spark.stop()