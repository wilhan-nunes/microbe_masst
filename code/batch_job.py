import logging
import sys
import argparse
from pathlib import Path
import sys
from pathlib import Path

# Add submodule code directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "microbe_masst" / "code"))

from masst_batch_client import run_on_usi_list_or_mgf_file
from masst_utils import DataBase

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main(
    input_file,
    output_file,
    min_cos=0.7,
    mz_tol=0.02,
    prec_tol=0.02,
    min_matched_signals=3,
    parallel_queries=5,
    skip_existing=True,
    analog=False,
    analog_mass_below=150,
    analog_mass_above=200,
):
    sep = "," if input_file.endswith("csv") else "\t"

    run_on_usi_list_or_mgf_file(
        in_file=input_file,
        out_file_no_extension=output_file,
        min_cos=min_cos,
        mz_tol=mz_tol,
        precursor_mz_tol=prec_tol,
        min_matched_signals=min_matched_signals,
        database=DataBase.metabolomicspanrepo_index_nightly,
        parallel_queries=parallel_queries,
        skip_existing=skip_existing,
        analog=analog,
        analog_mass_below=analog_mass_below,
        analog_mass_above=analog_mass_above,
        sep=sep,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run MASST batch processing")
    parser.add_argument("input_file", help="Input file (CSV, TSV, or MGF)")
    parser.add_argument("output_file", help="Output file path (without extension)")
    parser.add_argument(
        "--min-cos", type=float, default=0.7, help="Minimum cosine score"
    )
    parser.add_argument(
        "--mz-tol", type=float, default=0.02, help="fragment M/Z tolerance"
    )
    parser.add_argument(
        "--prec-tol", type=float, default=0.02, help="Precursor M/Z tolerance"
    )
    parser.add_argument(
        "--min-matched-signals", type=int, default=3, help="Minimum matched signals"
    )
    parser.add_argument(
        "--parallel-queries", type=int, default=5, help="Number of parallel queries"
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip existing results",
    )
    parser.add_argument(
        "--analog", action="store_true", default=False, help="Search for analogs"
    )
    parser.add_argument(
        "--analog-mass-below",
        type=float,
        default=150,
        help="Analog search mass below (Da)",
    )
    parser.add_argument(
        "--analog-mass-above",
        type=float,
        default=200,
        help="Analog search mass above (Da)",
    )

    args = parser.parse_args()

    try:
        main(
            args.input_file,
            args.output_file,
            args.min_cos,
            args.mz_tol,
            args.prec_tol,
            args.min_matched_signals,
            args.parallel_queries,
            args.skip_existing,
            args.analog,
            args.analog_mass_below,
            args.analog_mass_above,
        )
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error processing: {e}")
        sys.exit(1)
