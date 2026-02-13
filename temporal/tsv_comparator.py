import pandas as pd
import random

def compare_random_rows_tsv(
    file1: str,
    file2: str,
    start_row: int,
    end_row: int,
    n_samples: int,
    sep: str = "\t",
    random_seed: int | None = None,
):
    """
    Compara filas aleatorias de dos TSV dentro de un rango de índices.

    Parámetros
    ----------
    file1, file2 : rutas a los .tsv
    start_row    : índice de fila inicial (0-based, incluye esta fila)
    end_row      : índice de fila final (0-based, incluye esta fila)
    n_samples    : número de filas aleatorias a comparar
    sep          : separador del TSV (por defecto tabulador)
    random_seed  : semilla para reproducibilidad (opcional)
    """

    if random_seed is not None:
        random.seed(random_seed)

    # Leer los ficheros
    df1 = pd.read_csv(file1, sep=sep)
    df2 = pd.read_csv(file2, sep=sep)

    # Comprobación básica de tamaños
    if len(df1) != len(df2):
        print(f"AVISO: Los archivos tienen diferente número de filas: "
              f"{len(df1)} vs {len(df2)}")

    # Ajustar el rango al tamaño mínimo de ambos
    max_index = min(len(df1), len(df2)) - 1
    start_row = max(0, start_row)
    end_row = min(end_row, max_index)

    if start_row > end_row:
        raise ValueError(f"Rango inválido: start_row={start_row}, end_row={end_row}")

    # Lista de índices posibles en ese rango
    possible_indices = list(range(start_row, end_row + 1))

    if n_samples > len(possible_indices):
        raise ValueError(
            f"n_samples={n_samples} es mayor que el número de filas disponibles "
            f"en el rango ({len(possible_indices)})."
        )

    # Elegir índices aleatorios sin repetición
    sampled_indices = sorted(random.sample(possible_indices, n_samples))

    print(f"Comparando {n_samples} filas aleatorias entre los índices "
          f"{start_row} y {end_row} (0-based)...\n")

    all_equal = True

    for idx in sampled_indices:
        row1 = df1.iloc[idx]
        row2 = df2.iloc[idx]

        if row1.equals(row2):
            print(f"Fila {idx}: OK (las filas son idénticas)")
        else:
            all_equal = False
            print(f"\nFila {idx}: DIFERENTE")
            print("  → Valores en file1:")
            print(row1.to_dict())
            print("  → Valores en file2:")
            print(row2.to_dict())

    if all_equal:
        print("\nTodas las filas muestreadas son idénticas en ambos TSV.")
    else:
        print("\nHay diferencias en algunas de las filas muestreadas.")


if __name__ == "__main__":

    compare_random_rows_tsv(
        file1="final_data.tsv",
        file2="final_data_nt.tsv",
        start_row=0,      # fila mínima (0-based)
        end_row=100,      # fila máxima (0-based)
        n_samples=10,     # cuántas filas aleatorias comparar
        sep="\t",
        random_seed=42,   # opcional, para repetir siempre los mismos índices
    )
