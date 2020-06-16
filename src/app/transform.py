import arrow
from bs4 import BeautifulSoup


def keyword(text):
    """
    Formats keywords by 
    -lowercasing all letters 
    -replacing ' ' with '_'
    -replacing '-' with '_'
    """
    text = str(text).lower()
    text = text.replace("-", " ")
    return text


def remove_html(text):
    """
    Removes HTML
    """
    if type(text) == str:
        return BeautifulSoup(text, "html.parser").get_text()
    if type(text) == float:
        return BeautifulSoup(str(text), "html.parser").get_text()
    else:
        return [BeautifulSoup(x, "html.parser").get_text() for x in text]


def format_date(text):
    """
    Formats Date to 'YYYY-MM-DD'
    """
    time = arrow.get(text)
    time = time.format("YYYY-MM-DD")
    return time


cols_to_remove_html = ["title", "title_keyword", "description"]
date_cols = ["publication_date"]
keyword_cols = ["company", "title_keyword"]


def transform_df(df):
    """
    Takes in dataframe, applies functions to correct columns
    """
    # insert date column
    df["inserted_date"] = arrow.utcnow().format("YYYY-MM-DD")

    # remove html from columns
    for col in cols_to_remove_html:
        df[col] = df[col].apply(remove_html)

    # transforms date columns
    for col in date_cols:
        df[col] = df[col].apply(format_date)

    # transform keyword columns
    for col in keyword_cols:
        df[col] = df[col].apply(keyword)

    for col in ["latitude", "longitude"]:
        df[col] = df[col].fillna(0)

    return df
