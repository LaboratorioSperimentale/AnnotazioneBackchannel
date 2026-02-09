import sys
import csv

output_folder = "data/output_vert"

file_webanno = sys.argv[1]
file_vert = sys.argv[2]

tokens_to_update = {}

with open(file_webanno) as fin:
	for line in fin:
		if len(line.strip()) and not line.strip().startswith("#"):
			linesplit = line.strip().split("\t")
			webanno_id = linesplit[0]
			if not "." in webanno_id:
				_, _, form, backchannel, filler, _, _, repair, _, tok_id, _, _ = linesplit
				if backchannel != "_":
					tokens_to_update[tok_id] = backchannel
				if filler != "_":
					tokens_to_update[tok_id] = filler
				if repair != "_":
					tokens_to_update[tok_id] = repair


with open(file_vert) as fin:
    reader = csv.DictReader(fin, delimiter="\t")
    fieldnames = reader.fieldnames + ["backchannels"]
    writer = csv.DictWriter(open(f"{output_folder}/{file_vert.split('/')[-1]}", "w"),
                            fieldnames=fieldnames,
                            delimiter="\t",
                            restval="_")
    writer.writeheader()

    for row in reader:
        if row["token_id"] in tokens_to_update:
            row["backchannels"] = tokens_to_update[row["token_id"]]
        writer.writerow(row)