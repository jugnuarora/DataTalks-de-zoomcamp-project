import pandas as pd
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--input', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_file = args.input
output_file = args.output

try:
    df = pd.read_csv(input_file, sep='\t', header=None, encoding='latin1')
    df = df.iloc[:, [0, 1, 6, 9]]
    df.columns = ['formacode', 'description', 'field', 'generic_term']
    df.to_csv(output_file, index=False)
    print(f"CSV data written to {output_file}") #added print statement for confirmation.

except FileNotFoundError:
    print(f"Error: Input file '{input_file}' not found.")
except Exception as e:
    print(f"An error occurred: {e}")

