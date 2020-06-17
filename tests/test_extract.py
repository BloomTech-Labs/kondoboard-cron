from src.app.extract import adzuna, jobsearcher, monster_scraper
import pandas as pd


def test_adzuna():
    df = adzuna()
    assert list(df.columns) == [
        "id",
        "post_url",
        "title",
        "title_keyword",
        "tags",
        "description",
        "company",
        "publication_date",
        "latitude",
        "longitude",
        "city",
        "state",
    ]


def test_jobsearcher():
    df = jobsearcher()
    assert list(df.columns) == [
        "id",
        "post_url",
        "title",
        "title_keyword",
        "tags",
        "description",
        "company",
        "publication_date",
        "latitude",
        "longitude",
        "city",
        "state",
    ]


def test_monster():
    df = monster_scraper()
    assert list(df.columns) == [
        "id",
        "post_url",
        "title",
        "title_keyword",
        "tags",
        "description",
        "company",
        "publication_date",
        "latitude",
        "longitude",
        "city",
        "state",
    ]
