import yfinance as yf
import os
import pandas as pd

SYMBOLS = [
    "VCB.VN", "TCB.VN", "CTG.VN", "MBB.VN", "BID.VN",
    "FPT.VN", "HPG.VN", "HSG.VN", "MWG.VN", "PNJ.VN",
    "VHM.VN", "VIC.VN", "NVL.VN", "VNM.VN", "SAB.VN", "MSN.VN",
    "GAS.VN", "PLX.VN", "SSI.VN", "VND.VN", "HCM.VN"
]

def download_stock_data():
    """Tải dữ liệu cổ phiếu và lưu vào /opt/airflow/data/input"""
    input_dir = "/opt/airflow/data/input"
    os.makedirs(input_dir, exist_ok=True)

    for symbol in SYMBOLS:
        try:
            print(f"⬇️ Tải dữ liệu: {symbol}")
            df = yf.download(symbol, start="2023-01-01")
            if df.empty:
                print(f"⚠️ Không có dữ liệu cho {symbol}")
                continue

            df.reset_index(inplace=True)
            df.insert(0, "Symbol", symbol.replace(".VN", ""))

            out_path = os.path.join(input_dir, f"{symbol.replace('.VN', '')}.csv")
            df.to_csv(out_path, index=False)
            print(f"✅ Đã lưu: {out_path}")
        except Exception as e:
            print(f"❌ Lỗi khi tải {symbol}: {e}")

if __name__ == "__main__":
    download_stock_data()
