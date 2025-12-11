import csv
import re
import pathlib

def produce_sentence(sentence):

	yield f"#Text={sentence['text'].strip()}"
	for token in sentence["tokens"]:
		yield f"{token['id']}\t{token['span_chars']}\t{token['form']}\t{token['span']}\t{token['token_id']}\t{token['speaker_id']}\t{token['intonation']}\t{token['overlap_relation']}\t{token['overlap_type']}"
		# print(f"{token['id']}\t{token['span_chars']}\t{token['form']}\t{token['intonation']}\t{token['span']}\t{token['speaker_id']}\t{token['token_id']}")

def convert_file(file_input, output_path):

	with open(file_input) as fin, open(output_path, "w", encoding="utf-8") as fout:

		header = open("data/header.txt").read()
		fout.write(header)
		fout.write("\n\n\n")

		csv_reader = csv.DictReader(fin, delimiter="\t")

		sentence = {"text": "", "tokens": []}
		current_tuid = -1
		inception_id = 1
		inception_token_id = 1
		inception_token_offset = 0

		group_elements = {}

		for row in csv_reader:
			tu_id = int(row["tu_id"])

			if tu_id != current_tuid:
				if current_tuid != -1:
					for line in produce_sentence(sentence):
						fout.write(line + "\n")
					fout.write("\n")
					inception_id += 1
				sentence = {"text": "", "tokens": []}
				current_tuid = tu_id
				inception_token_id = 1

			sentence["text"] += f" {row['form']}"
			token = {
				"id": f"{inception_id}-{inception_token_id}",
				"span_chars": f"{inception_token_offset}-{inception_token_offset + len(row['form'])}",
				"form": row["form"],
				"span": row["span"].replace("[", "\\[").replace("]", "\\]"),
				"token_id": row["token_id"],
				"speaker_id": "_",
				"intonation": "_",
				"overlap_relation":"_",
				"overlap_type":"_"
			}

			if inception_token_id == 1:
				token["speaker_id"] = row["speaker"]

			jefferson_dict = {}
			jefferson_feats = row["jefferson_feats"].split("|")
			for feat in jefferson_feats:
				if feat != "_":
					key, value = feat.split("=", 1)
					jefferson_dict[key] = value

			if "Intonation" in jefferson_dict:
				token["intonation"] = jefferson_dict["Intonation"]

			overlaps = row["overlaps"].split(",")
			id_subtoken = 1
			for overlap in overlaps:
				if overlap != "_" and not "?" in overlap:
					pattern = r"^(\d+)-(\d+)\((\d+)\)$"
					match = re.match(pattern, overlap)
					# if match:
					start_offset = int(match.group(1))
					end_offset = int(match.group(2))
					group = int(match.group(3))
					if group not in group_elements:
						group_elements[group] = 1
					group_element = group_elements[group]
					group_elements[group] += 1
					if end_offset - start_offset == len(row["form"]):
						token["overlap_relation"] = f"*->{group+1}-{group_element}"
						token["overlap_type"] = f"*[{group+1}]"
					else:
						sentence["tokens"].append(token)
						token = {
						"id": f"{inception_id}-{inception_token_id}.{id_subtoken}",
						"span_chars": f"{inception_token_offset+start_offset}-{inception_token_offset + start_offset + end_offset}",
						"form": row["form"][start_offset:end_offset],
						"span": "_",
						"token_id": "_",
						"speaker_id": "_",
						"intonation": "_",
						"overlap_relation":"_",
						"overlap_type":"_"
						}
						id_subtoken += 1
						token["overlap_relation"] = f"*->{group+1}-{group_element}"
						token["overlap_type"] = f"*[{group+1}]"

			sentence["tokens"].append(token)
			inception_token_id += 1
			inception_token_offset += len(row["form"]) + 1  # +1 for the space

		for line in produce_sentence(sentence):
			fout.write(line + "\n")

def main(args):

	files_input = args.input_files
	output_folder = args.output_folder

	for file_input in files_input:
		basename = pathlib.Path(file_input).stem
		conv_id = basename.split(".")[0]
		output_path = pathlib.Path(output_folder) / f"{conv_id}.tsv"

		convert_file(file_input, output_path)


if __name__ == "__main__":

	import argparse

	parser = argparse.ArgumentParser(description="Convert .vert.tsv files to WebAnno TSV format.")
	parser.add_argument("--input-files", help="List of .vert.tsv files.", nargs='+', required=True)
	parser.add_argument("--output-folder", help="Output folder path.", default="data/output/")
	parser.set_defaults(func=main)

	args = parser.parse_args()
	if "func" not in args:
		parser.print_help()
		exit()
	args.func(args)


