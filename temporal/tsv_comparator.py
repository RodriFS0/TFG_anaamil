import pandas as pd
import random
from pathlib import Path


def find_bad_lines_tsv(path: str, sep: str = "\t", encoding: str = "utf-8"):
    """
    Detecta líneas con nº de columnas distinto al header.
    Devuelve: (expected_cols, bad_lines)
      - expected_cols: int
      - bad_lines: list of tuples (line_number_1based, n_fields_found, preview)
    """
    bad_lines = []
    p = Path(path)

    with p.open("r", encoding=encoding, errors="replace", newline="") as f:
        header = f.readline()
        if not header:
            return 0, []

        expected = len(header.rstrip("\n").split(sep))

        # line_number empieza en 2 porque ya leímos el header (línea 1)
        for line_number, line in enumerate(f, start=2):
            n_fields = len(line.rstrip("\n").split(sep))
            if n_fields != expected:
                preview = line[:200].replace("\n", "\\n").replace("\r", "\\r")
                bad_lines.append((line_number, n_fields, preview))

    return expected, bad_lines


def safe_read_tsv(path: str, sep: str = "\t", encoding: str = "utf-8"):
    """
    Intenta leer TSV en modo rápido. Si falla por ParserError,
    reintenta en modo tolerante (engine='python', on_bad_lines='skip').
    Devuelve: (df, mode_used)
      - mode_used: "fast" o "tolerant"
    """
    try:
        df = pd.read_csv(path, sep=sep, encoding=encoding)
        return df, "fast"
    except pd.errors.ParserError:
        df = pd.read_csv(
            path,
            sep=sep,
            encoding=encoding,
            engine="python",
            on_bad_lines="skip",
        )
        return df, "tolerant"


def compare_random_rows_tsv(
    file1: str,
    file2: str,
    start_row: int,
    end_row: int,
    n_samples: int,
    sep: str = "\t",
    random_seed: int | None = None,
    encoding: str = "utf-8",
    report_bad_lines: bool = True,
):
    """
    Compara filas aleatorias de dos TSV dentro de un rango de índices.
    Si hay problemas de parseo, usa lectura tolerante y genera reporte de líneas malas.
    """

    if random_seed is not None:
        random.seed(random_seed)

    # 1) Diagnóstico opcional: detectar líneas rotas y guardarlas a txt
    if report_bad_lines:
        for fpath in (file1, file2):
            expected, bad = find_bad_lines_tsv(fpath, sep=sep, encoding=encoding)
            out = Path(fpath).with_suffix(Path(fpath).suffix + ".bad_lines_report.txt")
            with out.open("w", encoding="utf-8") as rep:
                rep.write(f"Archivo: {fpath}\n")
                rep.write(f"Columnas esperadas (según header): {expected}\n")
                rep.write(f"Total líneas con nº de columnas distinto: {len(bad)}\n\n")
                rep.write("line_number(1-based)\tfields_found\tpreview\n")
                for ln, nf, prev in bad[:5000]:
                    rep.write(f"{ln}\t{nf}\t{prev}\n")
            # Si quieres ver algo por consola:
            if bad:
                print(f"[AVISO] {fpath}: {len(bad)} líneas con columnas inconsistentes.")
                print(f"        Reporte guardado en: {out}")
            else:
                print(f"[OK] {fpath}: no se detectaron líneas inconsistentes.")

    # 2) Leer ficheros (con fallback automático)
    df1, mode1 = safe_read_tsv(file1, sep=sep, encoding=encoding)
    df2, mode2 = safe_read_tsv(file2, sep=sep, encoding=encoding)

    print(f"\nLectura file1: {file1} -> modo {mode1}, filas={len(df1)}, cols={df1.shape[1]}")
    print(f"Lectura file2: {file2} -> modo {mode2}, filas={len(df2)}, cols={df2.shape[1]}")

    # 3) Aviso si tamaños diferentes (muy común si se han “skipped” líneas)
    if len(df1) != len(df2):
        print(
            f"\nAVISO: Los DataFrames tienen diferente número de filas "
            f"(probable salto de líneas malas): {len(df1)} vs {len(df2)}"
        )

    # 4) Alinear por columnas comunes (por si el orden cambia o hay diferencias de columnas)
    common_cols = [c for c in df1.columns if c in df2.columns]
    if not common_cols:
        raise ValueError("No hay columnas comunes entre los dos TSV. No se puede comparar.")

    # Reordenar y quedarnos solo con columnas comunes
    df1c = df1[common_cols].copy()
    df2c = df2[common_cols].copy()

    # 5) Ajustar rango al tamaño mínimo de ambos
    max_index = min(len(df1c), len(df2c)) - 1
    if max_index < 0:
        raise ValueError("Alguno de los archivos quedó vacío tras la lectura.")

    start_row = max(0, start_row)
    end_row = min(end_row, max_index)

    if start_row > end_row:
        raise ValueError(f"Rango inválido: start_row={start_row}, end_row={end_row}")

    possible_indices = list(range(start_row, end_row + 1))
    if n_samples > len(possible_indices):
        raise ValueError(
            f"n_samples={n_samples} es mayor que las filas disponibles en el rango ({len(possible_indices)})."
        )

    sampled_indices = sorted(random.sample(possible_indices, n_samples))

    print(
        f"\nComparando {n_samples} filas aleatorias entre los índices "
        f"{start_row} y {end_row} (0-based) sobre columnas comunes ({len(common_cols)})...\n"
    )

    all_equal = True

    for idx in sampled_indices:
        row1 = df1c.iloc[idx]
        row2 = df2c.iloc[idx]

        # Comparación robusta tratando NaN como iguales
        equal = row1.fillna("<NA>").equals(row2.fillna("<NA>"))

        if equal:
            print(f"Fila {idx}: OK (idéntica en columnas comunes)")
        else:
            all_equal = False
            diffs = {}
            for col in common_cols:
                v1 = row1[col]
                v2 = row2[col]
                # Igualdad robusta NaN
                if (pd.isna(v1) and pd.isna(v2)) or (v1 == v2):
                    continue
                diffs[col] = (v1, v2)

            print(f"\nFila {idx}: DIFERENTE (en {len(diffs)} columnas)")
            # Muestra hasta 25 diferencias para no inundar consola
            shown = 0
            for col, (v1, v2) in diffs.items():
                print(f"  - {col}: file1={v1!r} | file2={v2!r}")
                shown += 1
                if shown >= 25 and len(diffs) > 25:
                    print(f"  ... y {len(diffs) - 25} diferencias más")
                    break

    if all_equal:
        print("\nTodas las filas muestreadas son idénticas (en columnas comunes).")
    else:
        print("\nHay diferencias en algunas de las filas muestreadas (en columnas comunes).")


if __name__ == "__main__":
    compare_random_rows_tsv(
        file1="../RepoRT_classified_testOptimiced.tsv",
        file2="../RepoRT_classified_testOriginal.tsv",
        start_row=0,
        end_row=100,
        n_samples=10,
        sep="\t",
        random_seed=42,
        encoding="utf-8",
        report_bad_lines=True,
    )
