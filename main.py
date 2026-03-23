from pathlib import Path
import src.extraction as ext

# Defining Bronze Path
bronze_path = Path("data/1_Bronze")

# Ensuring Path exists
bronze_path.mkdir(exist_ok=True, parents=True)

# ------------------ Extraction using commit a499dd34c1372468f2335a370c5dd13cc3a72d90

url = "https://raw.githubusercontent.com/owid/co2-data/a499dd34c1372468f2335a370c5dd13cc3a72d90/owid-co2-data.csv"

if not any(bronze_path.iterdir()):
    print("Starting extraction ...")
    ext.extract(url)
else:
    print("Data already available. Skipping extraction ...")



