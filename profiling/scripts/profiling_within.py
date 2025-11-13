import nearust
import pandas as pd

if __name__ == "__main__":
    df = pd.read_csv("./test_files/cdr3b_10k_a.txt", header=None)
    _ = nearust.symdel(df[0])
