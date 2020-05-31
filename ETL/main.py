
from etl.extract import all_apis
from etl.transform import transform_df
from etl.load import load_query


def main():
    df = all_apis()
    df = transform_df(df)
    df = load_query(df)


if __name__ == "__main__":
    main()
