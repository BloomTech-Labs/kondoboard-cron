from src.app.transform import keyword, remove_html, format_date, transform_df
import pandas as pd
import numpy as np
import arrow


def test_keyword():
    """Tests keyword logic
    """

    text = "Hello keYword"
    expected = "hello keyword"
    actual = keyword(text)

    assert expected == actual


def test_remove_html(snapshot):
    text = """ 
    <html>
    <head>
    <title>
    A Simple HTML Document
    </title>
    </head>
    <body>
    <p>This is a very simple HTML document</p>
    <p>It only has two paragraphs</p>
    </body>
    </html>
    """
    actual = remove_html(text)
    snapshot.assert_match(actual)


def test_date():
    """Test date parsing in arrow
    """
    test_date = "2020-10-07"
    actual = format_date(test_date)
    assert actual == test_date

    fake_input = {
        "title": ["<strong>data engineer</strong>"],
        "title_keyword": ["<strong>DaTa-EnGiNeEr</strong>"],
        "description": ["<strong>We are looking for...</strong>"],
        "publication_date": ["1996-06-05"],
        "company": ["AmaZON"],
        "longitude": [np.NaN],
        "latitude": [0.0],
    }

    fake_output = {
        "title": ["data engineer"],
        "title_keyword": ["data engineer"],
        "description": ["We are looking for..."],
        "publication_date": ["1996-06-05"],
        "company": ["amazon"],
        "longitude": [0.0],
        "latitude": [0.0],
        "inserted_date": arrow.utcnow().format("YYYY-MM-DD"),
    }

    df_input = pd.DataFrame(data=fake_input)
    df_output = pd.DataFrame(data=fake_output)
    actual = transform_df(df_input)
    assert actual.equals(df_output)
