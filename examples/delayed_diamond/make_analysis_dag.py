#!/usr/bin/env python

from pathlib import Path
import glob

import htcondor
from htcondor import dags

analysis_dag = dags.DAG()


# This is the "count words in chunk" step, which now lives in the sub-DAG.
# The split will have run by the time this code executes.
# Therefore, we can inspect the directory to find out how many chunks were created.

# determine the number of files in this directory that match the pattern
num_chunks = len(glob.glob("words_*.txt"))

count_words = analysis_dag.layer(
    name="count_words",
    submit_description=htcondor.Submit(
        {
            "executable": "count_words.py",
            "arguments": "$(word_set)",
            "transfer_input_files": "words_$(word_set).txt",
            "output": "count_words_$(word_set).out",
            "error": "count_words_$(word_set).err",
        }
    ),
    vars=[{"word_set": str(n)} for n in range(num_chunks)],
)

# This is the "combine the counts from each chunk" step.
combine_counts = count_words.child_layer(
    name="combine_counts",
    submit_description=htcondor.Submit(
        {
            "executable": "combine_counts.py",
            "transfer_input_files": ", ".join(f"counts_{n}.txt" for n in range(num_chunks)),
            "output": "combine_counts.out",
            "error": "combine_counts.err",
        }
    ),
)

# We're done setting up the DAG, so we can write it out.
# The DAG input file itself as well as all of the submit descriptions will
# be written out to the specified directory.
# Here, we just write it out to the same directory that this file is in.
# If you write it out to a different directory, you may need to be careful
# about filepaths in your submit descriptions!
this_dir = Path(__file__).parent
dags.write_dag(analysis_dag, this_dir, dag_file_name="analysis.dag")
print(f"Wrote DAG files to {this_dir}")
