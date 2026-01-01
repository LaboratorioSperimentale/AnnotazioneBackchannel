import csv
from difflib import SequenceMatcher
from pathlib import Path

# =========================
# CONFIG
# =========================

FILE_ID = "PBA001"

GENERATED_FILE = "data/input-diarize-c1/PBA001_diarize-c1_normalized.txt"
ORIGINAL_FILE = "data/input-kiparla/PBA001.txt"

OUTPUT_DIR = Path("alignments")
OUTPUT_DIR.mkdir(exist_ok=True)

OUT_CONVERSATION = OUTPUT_DIR / f"alignment_{FILE_ID}_conversation.tsv"

# =========================
# UTILS
# =========================

def parse_lines(lines):
    """
    Parses lines of the form:
    SPEAKER<TAB>text

    Returns a list of tuples:
    [(speaker, text), ...] preserving order
    """
    parsed = []
    for line in lines:
        line = line.strip()
        if "\t" not in line:
            continue
        speaker, text = line.split("\t", 1)
        parsed.append((speaker, text.strip()))
    return parsed


def align_text(hyp_text, ref_text):
    """
    Aligns hypothesis and reference word-by-word
    using SequenceMatcher.

    Returns:
        aligned_hyp (list of str)
        aligned_ref (list of str)
    """
    hyp_words = hyp_text.split()
    ref_words = ref_text.split()

    matcher = SequenceMatcher(None, hyp_words, ref_words)

    aligned_hyp = []
    aligned_ref = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            aligned_hyp.extend(hyp_words[i1:i2])
            aligned_ref.extend(ref_words[j1:j2])

        elif tag == "replace":
            length = max(i2 - i1, j2 - j1)
            aligned_hyp.extend(hyp_words[i1:i2] + ["_"] * (length - (i2 - i1)))
            aligned_ref.extend(ref_words[j1:j2] + ["_"] * (length - (j2 - j1)))

        elif tag == "delete":
            aligned_hyp.extend(hyp_words[i1:i2])
            aligned_ref.extend(["_"] * (i2 - i1))

        elif tag == "insert":
            aligned_hyp.extend(["_"] * (j2 - j1))
            aligned_ref.extend(ref_words[j1:j2])

    return aligned_hyp, aligned_ref


# =========================
# LOAD FILES
# =========================

with open(GENERATED_FILE, encoding="utf-8") as f:
    generated_lines = parse_lines(f.readlines())

with open(ORIGINAL_FILE, encoding="utf-8") as f:
    original_lines = parse_lines(f.readlines())

# =========================
# BUILD CONVERSATION ORDER
# =========================

# We iterate over the ORIGINAL transcript,
# because it defines the real conversational order
rows = []
rows_by_speaker = {}

gen_idx = 0  # pointer in generated transcript

for spk, ref_text in original_lines:

    # collect all generated segments for this speaker
    hyp_segments = []
    while gen_idx < len(generated_lines) and generated_lines[gen_idx][0] == spk:
        hyp_segments.append(generated_lines[gen_idx][1])
        gen_idx += 1

    hyp_text = " ".join(hyp_segments)

    if not hyp_text.strip() and not ref_text.strip():
        continue

    aligned_hyp, aligned_ref = align_text(hyp_text, ref_text)

    for r, h in zip(aligned_ref, aligned_hyp):
        row = [spk, r, h]
        rows.append(row)
        rows_by_speaker.setdefault(spk, []).append(row)

# =========================
# WRITE CONVERSATION FILE
# =========================

with open(OUT_CONVERSATION, "w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f, delimiter="\t")
    writer.writerow(["Speaker", "Gold", "Generated"])
    writer.writerows(rows)

# =========================
# WRITE PER-SPEAKER FILES
# =========================

for spk, spk_rows in rows_by_speaker.items():
    out_spk = OUTPUT_DIR / f"alignment_{FILE_ID}_{spk}.tsv"

    with open(out_spk, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["Speaker", "Gold", "Generated"])
        writer.writerows(spk_rows)


