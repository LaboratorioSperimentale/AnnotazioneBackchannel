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


results = {}

for entry in FILES:
    with open(entry["generated"], encoding="utf-8") as f:
        gen_lines = f.readlines()

    with open(entry["original"], encoding="utf-8") as f:
        gold_lines = f.readlines()

    # speakers in order of appearance in GOLD
    speakers = []
    for line in gold_lines:
        if "\t" in line:
            spk = line.split("\t", 1)[0]
            if spk not in speakers:
                speakers.append(spk)

    speaker_acc = {}
    global_ref, global_hyp = [], []

    for spk in speakers:
        hyp_text = extract_speaker_text(gen_lines, spk)
        ref_text = extract_speaker_text(gold_lines, spk)

        if not hyp_text or not ref_text:
            continue

        aligned_hyp, aligned_ref = align_text(hyp_text, ref_text)
        speaker_acc[spk] = accuracy_score(aligned_ref, aligned_hyp)

        global_ref.extend(aligned_ref)
        global_hyp.extend(aligned_hyp)

    results[entry["file_id"]] = {
        "overall": accuracy_score(global_ref, global_hyp),
        "speakers": speaker_acc,
    }

# =========================
# WRITE SUMMARY
# =========================

all_speakers = sorted(
    {s for r in results.values() for s in r["speakers"]}
)

with open(SUMMARY_FILE, "w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f, delimiter="\t")
    writer.writerow(["File", "Overall"] + all_speakers)

    for fid, res in results.items():
        row = [fid, f"{res['overall']*100:.2f}%"]
        for spk in all_speakers:
            row.append(
                f"{res['speakers'][spk]*100:.2f}%" if spk in res["speakers"] else "-"
            )
        writer.writerow(row)