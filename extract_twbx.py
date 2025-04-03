import zipfile
import os

# Directory settings – adjust as needed or import from a config file.
BASE_DIR = os.getcwd()
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
EXTRACT_DIR = os.path.join(OUTPUT_DIR, "extracted")
os.makedirs(EXTRACT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

def extract_twbx(twbx_file):
    """Extracts a .twbx file into output/extracted/."""
    try:
        with zipfile.ZipFile(twbx_file, 'r') as zip_ref:
            zip_ref.extractall(EXTRACT_DIR)
        print(f"✅ Extracted {twbx_file} to {EXTRACT_DIR}")
    except zipfile.BadZipFile:
        print(f"❌ Error: {twbx_file} is not a valid ZIP file.")
    except Exception as e:
        print(f"❌ Error extracting {twbx_file}: {e}")

# Expose directories for other modules
def get_directories():
    return BASE_DIR, OUTPUT_DIR, EXTRACT_DIR
