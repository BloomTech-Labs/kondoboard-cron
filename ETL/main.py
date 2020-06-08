from etl.extract import merge_all_apis
from etl.transform import transform_df
from etl.load import query


def main():
    df = merge_all_apis()
    df = transform_df(df)
    df = query(df)


if __name__ == "__main__":
    main()
