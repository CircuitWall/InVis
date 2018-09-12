import numpy as np
import pandas as pd
import holoviews as hv

hv.extension('matplotlib')
hv.output(size=200, fig='svg')


def plot_chord(data_input, from_column, to_column, **kwargs):
    if from_column in data_input.columns and to_column in data_input.columns:
        freq_input = pd.crosstab(data_input[from_column], data_input[to_column])
        freq_input = freq_input.reset_index()
        print("frequence table of " + " " + from_column + " " + to_column)
        print(freq_input)

        chord_input = pd.melt(freq_input, id_vars=from_column)

        chord_index_from = pd.DataFrame(data_input[from_column].unique())
        chord_index_to = pd.DataFrame(data_input[to_column].unique())
        chord_index = chord_index_from.append(chord_index_to, ignore_index=True).dropna()
        chord_dict = chord_index[0].to_dict()
        chord_dict = dict((v, k) for k, v in chord_dict.items())

        chord_input.replace(chord_dict, inplace=True)

        return hv.Chord(chord_input)

    else:
        print("re-format the data input, give three arguments: input dataframe, from column, to column")
