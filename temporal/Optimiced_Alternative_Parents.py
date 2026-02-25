import pandas as pd
import re
from glob import glob
from pathlib import Path
from itertools import islice
from temporal.Update_RepoRT import ensure_processed_data_updated


def optimiced_alternative_parents(
    classified_path="sampled_classified.tsv",
    out_path="RepoRT_classified_testOptimiced.tsv",
    lines_per_block=1000,
    encoding="utf-8"
):
    #processed_path = ensure_processed_data_updated()
    processed_path=Path("external/RepoRT/processed_data/processed_data")
    directory = list(processed_path.glob("*/*.tsv"))
    results = []

    for files in directory:
        if re.search(r"_rtdata_canonical_success.tsv", str(files)):
            df_rt = pd.read_csv(files, sep="\t", header=0, encoding=encoding)
            df_rt['study'] = Path(files).stem.split('_')[0]  # Add study column
            # Load gradient
            gradient_path = files.parent / f"{df_rt['study'].iloc[0]}_gradient.tsv"
            if gradient_path.exists():
                df_grad = pd.read_csv(gradient_path, sep="\t", header=0, encoding=encoding)
                # Convert to string
                grad_str = df_grad.to_csv(sep="\t", index=False)
                df_rt['gradient'] = grad_str
            else:
                df_rt['gradient'] = ""
            results.append(df_rt)

    print("TSVs encontrados por glob:", len(directory))
    print("TSVs que matchean el patrón:", len(results))

    if not results:
        return None

    df_concat = pd.concat(results, axis=0, ignore_index=True)

    inchikey_series = df_concat["inchikey.std"].astype(str).str.strip()

    classified_path = Path(classified_path)
    if not classified_path.exists():
        raise FileNotFoundError(f"No encuentro {classified_path.resolve()}")

    out_path = Path(out_path)
    if out_path.exists():
        out_path.unlink()

    wrote_header = False
    total_rows = 0

    with open(classified_path, "r", encoding=encoding, errors="replace") as f:
        block_number = 0

        while True:
            block_lines = list(islice(f, lines_per_block))
            if not block_lines:
                break

            block_number += 1
            df_list_block = []

            for line in block_lines:
                line = line.rstrip("\n")
                if not line:
                    continue

                lines = line.split("\t")  # EXACTAMENTE igual que el original
                key = lines[0].strip()

                mask = inchikey_series == key  # Changed to exact match
                if not mask.any():
                    continue

                df_query = df_concat.loc[mask].copy()
                df_query.loc[:, "inchikey.std"] = key

                df_at = pd.DataFrame(lines).transpose()  # EXACTAMENTE igual
                merged = pd.merge(df_query, df_at, right_on=0, left_on="inchikey.std")

                df_list_block.append(merged)

            if df_list_block:
                df_block = pd.concat(df_list_block, ignore_index=True)

                df_block.to_csv(
                    out_path,
                    sep="\t",
                    index=False,
                    mode="a",
                    header=(not wrote_header)
                )
                wrote_header = True
                total_rows += len(df_block)

            print(f"Bloque {block_number} procesado")

    if not wrote_header:
        print("No hubo matches.")
        return None

    print("Alternative Parents Proccess finished")
    fix_header_extend(out_path, encoding=encoding)
    print("Total filas escritas:", total_rows)
    print("Guardado en:", out_path.resolve())


    return out_path.resolve()

    
from pathlib import Path

def fix_header_extend(path, encoding="utf-8"):
    path = Path(path)

    # 1) calcular el máximo número de columnas reales en TODO el archivo
    max_cols = 0
    with open(path, "r", encoding=encoding, errors="replace") as f:
        for line in f:
            n = line.rstrip("\n").count("\t") + 1
            if n > max_cols:
                max_cols = n

    # 2) leer el header actual
    with open(path, "r", encoding=encoding, errors="replace") as f:
        header_line = f.readline().rstrip("\n")
    header = header_line.split("\t") if header_line else []

    if len(header) == 0:
        raise ValueError("El archivo está vacío o no tiene primera línea.")

    if len(header) >= max_cols:
        print(f"Header ya tiene {len(header)} columnas (max en datos = {max_cols}). No cambio nada.")
        return

    # 3) decidir cómo nombrar las columnas faltantes
    # Si ya hay columnas numéricas al final (p.ej. '0','1',...'93'), continuamos: '94','95',...
    numeric = []
    for h in header:
        if h.isdigit():
            numeric.append(int(h))
    start_num = (max(numeric) + 1) if numeric else 0

    missing = max_cols - len(header)
    extra = [str(start_num + i) for i in range(missing)]
    new_header = header + extra

    # 4) reescribir a un temporal: nuevo header + resto del archivo sin tocar
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(path, "r", encoding=encoding, errors="replace") as fin, \
         open(tmp, "w", encoding=encoding) as fout:
        _ = fin.readline()  # saltar header antiguo
        fout.write("\t".join(new_header) + "\n")
        for line in fin:
            fout.write(line)

    tmp.replace(path)
    print(f"Header ampliado: {len(header)} -> {len(new_header)} columnas (max en datos = {max_cols}).")


if __name__ == "__main__":
    optimiced_alternative_parents()
