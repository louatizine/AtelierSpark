import time
import glob
import os
import pandas as pd
import matplotlib.pyplot as plt

OUTPUT_DIR = "output/errors_json"
os.makedirs(OUTPUT_DIR, exist_ok=True)
plt.ion()
fig, ax = plt.subplots()

def latest_json():
    files = glob.glob(os.path.join(OUTPUT_DIR, "*.json"))
    return max(files, key=os.path.getmtime) if files else None

print("Starting live plot. Close the plot window to stop.")
try:
    while True:
        path = latest_json()
        if path:
            try:
                df = pd.read_json(path)
                ax.clear()
                if not df.empty:
                    df_sorted = df.sort_values("status")
                    ax.bar(df_sorted["status"].astype(str), df_sorted["count"])
                    ax.set_title("HTTP errors (snapshot)")
                    ax.set_xlabel("status")
                    ax.set_ylabel("count")
                else:
                    ax.text(0.5, 0.5, "no errors yet", ha="center")
                plt.pause(0.1)
            except Exception as e:
                # transient IO / parsing errors can occur while Spark writes files
                pass
        time.sleep(2)
except KeyboardInterrupt:
    print("Plot stopped by user.")