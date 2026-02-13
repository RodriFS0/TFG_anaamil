input_file = "all_classified.tsv"
output_file = "sampled_classified.tsv"
n_lines = 10000

with open(input_file, "r", encoding="utf-8") as infile, \
        open(output_file, "w", encoding="utf-8") as outfile:
    for i, line in enumerate(infile):
        if i >= n_lines:
            break
        outfile.write(line)
print("Archivo sampled.tsv creado con éxito")
