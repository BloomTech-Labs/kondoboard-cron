from etl.transform import keyword, remove_html, format_date

def test_keyword():
    """Tests keyword logic
    """

    text = "hello keyword"
    expected = "hello_keyword"
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
    test_date= "2020-10-07"
    actual = format_date(test_date)
    assert actual == test_date