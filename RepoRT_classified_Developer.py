import argparse
import shutil
from pathlib import Path
from temporal.Optimiced_Alternative_Parents import optimiced_alternative_parents

def RepoRT_classified_Developer(classified_path, lines_per_block, output_file="repoRT_joint.tsv"):
    final_file = optimiced_alternative_parents(
        classified_path=classified_path,
        lines_per_block=lines_per_block, 
        out_path=output_file
    )



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
        "--output_file",
        type=str,
        default="repoRT_joint.tsv",
        help="Path to output file in tsv format"
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
