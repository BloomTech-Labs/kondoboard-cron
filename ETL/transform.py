import arrow
from bs4 import BeautifulSoup


def keyword(text):
    """
    Formats keywords by 
    -lowercasing all letters 
    -replacing ' ' with '_'
    -replacing '-' with '_'
    """
    text = text.lower()
    text = text.replace(" ", "_")
    text = text.replace("-", "_")
    return text


def remove_html(text):
    """
    Removes HTML
    """
    return BeautifulSoup(text).get_text()


def format_date(text):
    """
    Formats Date to 'YYYY-MM-DD'
    """
    time = arrow.get(text)
    time = time.format("YYYY-MM-DD")
    return time


# TODO:
# Change it so that we have a list of the columns that need
# the specific functions applied to them so that we can just add them
# to each one

# ex:
# col_to_remove_html = ['title', 'title_keyword', 'description']
# for col in col_to_remove_html:
#     df[col] = df[col].apply(remove_html)
def transform_df(df):
    """
    Takes in dataframe, applies functions to correct columns
    """
    # insert date column
    df["inserted_date"] = arrow.utcnow().format("YYYY-MM-DD")

    # transform ID column
    df["id"] = df["id"].apply(adzuna_id)

    # remove HTML from columns
    df["title"] = df["title"].apply(remove_html)
    df["title_keyword"] = df["title_keyword"].apply(remove_html)
    df["description"] = df["description"].apply(remove_html)

    # transforms date columns
    df["publication_date"] = df["publication_date"].apply(format_date)

    # transform keyword columns
    df["company"] = df["company"].apply(keyword)
    df["title_keyword"] = df["title_keyword"].apply(keyword)

    return df
