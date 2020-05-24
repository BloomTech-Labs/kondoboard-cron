from etl.extract import request_to_df
from etl.transform import transform_df
from etl.load import load_query

def main():
    df = request_to_df()
    df = transform_df(df)
    df = load_query(df)


if __name__ == "__main__":
    main()