import argparse
import shutil
from pathlib import Path
import tkinter as tk
from tkinter import filedialog
from temporal.Optimiced_Alternative_Parents import optimiced_alternative_parents

def RepoRT_classified_Developer(classified_path, lines_per_block):
    final_file = optimiced_alternative_parents(
        classified_path=classified_path,
        lines_per_block=lines_per_block
    )

    if final_file:
        save_file_as(final_file)


def save_file_as(source_path):
    source_path = Path(source_path)

    if not source_path.exists():
        raise FileNotFoundError(f"File not found: {source_path}")

    root = tk.Tk()
    root.withdraw()

    destination = filedialog.asksaveasfilename(
        title="Save RepoRT file as...",
        defaultextension=".tsv",
        initialfile=source_path.name,
        filetypes=[("TSV files", "*.tsv"), ("All files", "*.*")]
    )

    if not destination:
        print("Save cancelled.")
        return None

    destination_path = Path(destination)
    shutil.copy2(source_path, destination_path)

    print(f"File saved to: {destination_path}")
    return destination_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate RepoRT classified file and choose where to save it."
    )

    parser.add_argument(
        "--classified",
        type=str,
        default="all_classified.tsv",
        help="Path to sampled_classified.tsv file"
    )

    parser.add_argument(
        "--blocksize",
        type=int,
        default=6000000,
        help="Number of lines per processing block"
    )

    args = parser.parse_args()

    RepoRT_classified_Developer(
        classified_path=args.classified,
        lines_per_block=args.blocksize
    )
