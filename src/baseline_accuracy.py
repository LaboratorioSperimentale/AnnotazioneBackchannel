import csv
from difflib import SequenceMatcher
from sklearn.metrics import accuracy_score
from pathlib import Path


# Defining paths and files
FILES = [
    {
        "file_id": "PBA001",
        "generated": "data/input-diarize-c1/PBA001_diarize-c1_normalized.txt",
        "original": "data/input-kiparla/PBA001.txt",
    }
]

OUTPUT_DIR = Path("alignments")
OUTPUT_DIR.mkdir(exist_ok=True)
SUMMARY_FILE = OUTPUT_DIR / "accuracy_summary.tsv"


def extract_speaker_text(transcript_lines, speaker_id):
    """
    Extracts and concatenates all text for a given speaker from the transcript lines.
    Returns a single string with the speaker's text.
    """
    lines = [
        line.split("\t", 1)[1].strip()
        for line in transcript_lines
        if line.startswith(speaker_id + "\t")
    ]
    return " ".join(lines)


def align_text(hyp_text, ref_text):
    """
    Aligns the generated and reference texts word by word using SequenceMatcher to align identical, substituted, deleted and inserted words.
    
    Args:
        hyp_text (str): The generated text from the model.
        ref_text (str): The reference text to align against.
    
    Returns:
        aligned_hyp (list): List of words from the generated text, aligned with gaps for deletions/insertions.
        aligned_ref (list): List of words from the reference text, aligned with gaps for deletions/insertions.
    """
    hyp_words = hyp_text.split()
    ref_words = ref_text.split()

    matcher = SequenceMatcher(None, hyp_words, ref_words)

    aligned_hyp = []
    aligned_ref = []

    # iterate over the blocks identified by SequenceMatcher to build aligned lists
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            aligned_hyp.extend(hyp_words[i1:i2])
            aligned_ref.extend(ref_words[j1:j2])

        elif tag == "replace": # substitutions 
            length = max(i2 - i1, j2 - j1)
            aligned_hyp.extend(hyp_words[i1:i2] + [""] * (length - (i2 - i1)))
            aligned_ref.extend(ref_words[j1:j2] + [""] * (length - (j2 - j1)))

        elif tag == "delete": # deleted words in the Whisper output
            aligned_hyp.extend(hyp_words[i1:i2])
            aligned_ref.extend([""] * (i2 - i1))

        elif tag == "insert": # words added in the Whisper output
            aligned_hyp.extend([""] * (j2 - j1))
            aligned_ref.extend(ref_words[j1:j2])

    return aligned_hyp, aligned_ref


def compute_accuracies_for_file(file_id, generated_lines, original_lines):
    """
    Computes accuracy for each speaker and overall accuracy.
    
    Args:
        file_id (str): Identifier for the file.
        generated_lines (list): Lines from the generated transcript.
        original_lines (list): Lines from the original transcript.
    
    Returns:
        dict: A dictionary with (a) overall accuracy and (b) per-speaker accuracies.
    """
    speakers = set()

    # this loop identifies all speakers present in either transcript
    for line in generated_lines + original_lines:
        if "\t" in line:
            speakers.add(line.split("\t", 1)[0])

    speaker_accuracies = {}
    global_ref = []
    global_hyp = []

    # calculate accuracy per speaker
    for speaker in sorted(speakers):
        hyp_text = extract_speaker_text(generated_lines, speaker)
        ref_text = extract_speaker_text(original_lines, speaker)

        if not hyp_text.strip() or not ref_text.strip():
            continue # skip if either text is empty

        aligned_hyp, aligned_ref = align_text(hyp_text, ref_text)

        acc = accuracy_score(aligned_ref, aligned_hyp)
        speaker_accuracies[speaker] = acc

        global_ref.extend(aligned_ref)
        global_hyp.extend(aligned_hyp)

    # calculate overall accuracy
    global_accuracy = accuracy_score(global_ref, global_hyp) if global_ref else 0.0

    return {
        "file": file_id,
        "overall": global_accuracy,
        "speakers": speaker_accuracies,
    }

# Main processing loop, processing each file and computing accuracies
results = {}

for entry in FILES:
    file_id = entry["file_id"]

    with open(entry["generated"], encoding="utf-8") as f:
        generated_lines = f.readlines()

    with open(entry["original"], encoding="utf-8") as f:
        original_lines = f.readlines()

    results[file_id] = compute_accuracies_for_file(
        file_id, generated_lines, original_lines
    )

# Creating the summary TSV file

# Collect all unique speakers across all files
all_speakers = set()
for res in results.values():
    all_speakers.update(res["speakers"].keys())

all_speakers = sorted(all_speakers)

with open(SUMMARY_FILE, "w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f, delimiter="\t")

    header = ["File", "Overall"] + all_speakers
    writer.writerow(header)

    for file_id, res in results.items():
        row = [file_id, f"{res['overall'] * 100:.2f}%"]

        for speaker in all_speakers:
            if speaker in res["speakers"]:
                row.append(f"{res['speakers'][speaker] * 100:.2f}%")
            else:
                row.append("-")

        writer.writerow(row)
