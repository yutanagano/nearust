import nearust
import pandas as pd

if __name__ == "__main__":
    query = pd.read_csv("./test_files/cdr3b_1m_a.txt", header=None)
    ref = pd.read_csv("./test_files/cdr3b_1m_b.txt", header=None)
    _ = nearust.symdel(query[0], ref[0])
