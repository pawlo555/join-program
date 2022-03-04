import sys
import pandas as pd
import numpy as np
from enum import Enum

CHUNK_SIZE = 16


def main() -> None:
    assert len(sys.argv) == 4 or len(sys.argv) == 5, "Wrong argument number"
    if len(sys.argv) == 5:
        join(sys.argv[1:-1], join_type=JoinType[str(sys.argv[-1]).upper()])
    else:
        join(sys.argv[1:])


class JoinType(Enum):
    INNER = 1
    LEFT = 2
    RIGHT = 3


def join(params, join_type: JoinType = JoinType.INNER) -> None:
    first_file = params[0]
    second_file = params[1]
    column_name = params[2]

    if join_type is JoinType.INNER:
        inner_join(first_file, second_file, column_name)
    elif join_type is JoinType.LEFT:
        left_join(first_file, second_file, column_name)
    else:
        right_join(first_file, second_file, column_name)


def inner_join(first_file: str, second_file: str, column_name: str) -> None:
    first_reader = pd.read_csv(first_file, chunksize=CHUNK_SIZE)
    second_reader = pd.read_csv(second_file, chunksize=CHUNK_SIZE)

    # we will do many inner joins on small amount of data and then we will get all results
    for first_chunk in first_reader:
        for second_chunk in second_reader:
            joined = first_chunk.merge(second_chunk, on=column_name)
            print_dataframe(joined)


def left_join(first_file: str, second_file: str, column_name: str) -> None:
    first_reader = pd.read_csv(first_file, chunksize=CHUNK_SIZE)
    second_reader = pd.read_csv(second_file, chunksize=CHUNK_SIZE)

    second_names = pd.read_csv(second_file, nrows=1).columns.tolist()
    second_names.remove(column_name)

    for first_chunk in first_reader:
        first_chunk_used: pd.DataFrame = first_chunk.copy()  # we need to keep data that are only on left side
        for second_chunk in second_reader:
            joined = first_chunk.merge(second_chunk, on=column_name)
            print_dataframe(joined)
            pd.merge(first_chunk_used, second_chunk, on=[column_name], how="outer", indicator=True
                     ).query('_merge=="left_only"')  # get data that are only on left side -
            # if there are elements to join in two chunks we delete this row, because we already printed it
        first_chunk_used[second_names] = np.nan
        print_dataframe(first_chunk_used)


def right_join(first_file: str, second_file: str, column_name: str) -> None:
    left_join(second_file, first_file, column_name)


def print_dataframe(df: pd.DataFrame) -> None:
    if len(df) > 0:
        print(df.to_string(header=print_dataframe.header, index=False))
        print_dataframe.header = False


print_dataframe.header = True

if __name__ == '__main__':
    main()
