#!/usr/bin/env python

from pathlib import Path

import htcondor
from htcondor import dags

# We will split words.txt into five chunks.
NUM_CHUNKS = 5

# Start by creating the DAG object itself.
# This object "holds" the DAG information.
# Meta-information like DAGMan configuration, the location of the node status
# file, etc., lives on this object.
# It's methods are used to create node layers and possibly subDAGs.
diamond = dags.DAG()

# This is the "split" step.
# It has no parent layer, so it is a root layer of the DAG.
# Root layers are created from the DAG object itself.
split_words = diamond.layer(
    name="split_words",
    submit_description=htcondor.Submit(
        {
            "executable": "split_words.py",
            "arguments": str(NUM_CHUNKS),
            "transfer_input_files": "words.txt",
            "output": "split_words.out",
            "error": "split_words.err",
        }
    ),
)

# This is the "count words in chunk" step.
# It is a child layer of the "split" layer, so we can create it by calling
# the "child" method on the split layer object we got above.
# A single real DAGMan node will be created for each element in the list passed
# to vars.
# Because NUM_CHUNKS is 5, this will create 5 DAGMan nodes, one to process
# each chunk created by the previous layer.
count_words = split_words.child_layer(
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
    vars=[{"word_set": str(n)} for n in range(NUM_CHUNKS)],
)

# This is the "combine the counts from each chunk" step.
# Since it can't run until all the chunks are done, we create it as a child
# of the previous step.
# It's input files are all of the output files from the previous step, which
# is easy in this case because we know the naming scheme.
combine_counts = count_words.child_layer(
    name="combine_counts",
    submit_description=htcondor.Submit(
        {
            "executable": "combine_counts.py",
            "transfer_input_files": ", ".join(f"counts_{n}.txt" for n in range(NUM_CHUNKS)),
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
dags.write_dag(diamond, this_dir)
print(f"Wrote DAG files to {this_dir}")
