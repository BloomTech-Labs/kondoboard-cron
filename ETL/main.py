#TODO : change from adzuna to merge function
from etl.extract import adzuna
from etl.transform import transform_df
from etl.load import load_query

def main():
    df = adzuna()
    df = transform_df(df)
    df = load_query(df)


if __name__ == "__main__":
    main()