import re
import string

# Loading the file
file_path = "data/input-diarize-c1/PBA001_diarize-c1.txt"

with open(file_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

normalized_lines = []
for line in lines:
    line = line.strip()
    if not line:
        continue
    match = re.match(r'^([A-Z0-9]+)\s+(.*)', line) # matches speaker + text
    if match:
        speaker_id = match.group(1)
        text = match.group(2)
        
        text = text.lower()
        text = text.translate(str.maketrans("", "", string.punctuation)) #removes punctuation
        text = re.sub(r"\s+", " ", text).strip() # normalize spaces
        normalized_lines.append(f"{speaker_id}\t{text}\n") # final normalized line
    else:
        normalized_lines.append(line + "\n")

# Produce the normalized file
output_path = file_path.replace(".txt", "_normalized.txt")
with open(output_path, "w", encoding="utf-8") as f:
    f.writelines(normalized_lines)