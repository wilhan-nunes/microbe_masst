import logging
import sys

import masst_batch_client
import masst_utils

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

files = [
    # (r"../test_mgf.mgf", "../output/nina_"),
     (r"../11192019_gnps_export.mgf", "/Volumes/Helena/Nina_MASST/nina_"),
    # (r"../examples/all_compound_usi.txt", "../output/abubaker/test_"),
]

if __name__ == "__main__":
    for file, out_file in files:
        try:
            logger.info("Starting new job for input: {}".format(file))
            sep = (
                "," if file.endswith("csv") else "\t"
            )  # only used if tabular format not for mgf
            logger.debug("Resolved separator for input %s -> %r", file, sep)
            logger.info("Output prefix will be: %s", out_file)
            masst_batch_client.run_on_usi_list_or_mgf_file(
                in_file=file,
                out_file_no_extension=out_file,
                min_cos=0.7,
                mz_tol=0.02,
                precursor_mz_tol=0.02,
                min_matched_signals=3,
                database=masst_utils.DataBase.metabolomicspanrepo_index_latest,
                parallel_queries=10,
                skip_existing=True,
                analog=False,
                sep=sep,
            )
        except Exception as e:
            logger.exception("Job failed for input %s: %s", file, e)
            # Fail fast so we can see the error instead of silently succeeding
            sys.exit(1)
    # exit with OK
    sys.exit(0)
