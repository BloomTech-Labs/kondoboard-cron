import arrow

def keyword(text):
    """
    Formats keywords by 
    -lowercasing all letters 
    -replacing ' ' with '_'
    -replacing '-' with '_'
    """
    text = text.lower()
    text = text.replace(' ', '_')
    text = text.replace('-', '_')
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
    time = time.format('YYYY-MM-DD')
    return time

# this funciton needs to be changed so that it can check for the
# API that we have and change it based on that.. just not sure
# how that works with the apply funciton. don't want to have parameters?
def adzuna_id(text):
    """
    Formats ADZUNA API IDs to A + original ID
    """
    return "A" + str(text)

def transform(df):
    
    # insert date column
    df['inserted_date'] = arrow.utcnow().format("YYYY-MM-DD")

    # transform ID column
    df['id'] = df['id'].apply(adzuna_id)

    # remove HTML from columns
    df['title'] = df['title'].apply(remove_html)
    df['title_keyword'] = df['title_keyword'].apply(remove_html)

    # transforms date columns
    df['publication_date'] = df['publication_date'].apply(format_date)

    return df

    


