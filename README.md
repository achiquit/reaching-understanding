# reaching-understanding
The code supporting the Reaching Understanding study with the Deepest Beliefs Lab at UNC

Code to process a collection of reddit comments gathered using PushShift and save a .csv file for each post in the collection containing all dyadic conversations with a minimum of four responses. 

The collecting_data.py file can be run with some tweaks to the global variables to process the entire collection in one go.

Refer to the processing_data.ipynb file for more descriptions, debugging, and options for piecemealing the process.

The code could be improved by vectorizing the process, but we chose to leave it as-is after we extracted what data we needed after leaving the code running for several days.
