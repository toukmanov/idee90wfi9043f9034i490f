import pytest

from main import Document, TProcessor


def test_main():
    state = {}
    processor = TProcessor()

    doc1 = Document(url="doc1", pub_date=10, fetch_time=20, text="text1")
    out_doc = processor.process(doc1, state)
    input1 = doc1
    input1.first_fetch_time = doc1.fetch_time
    assert out_doc == input1

    doc2 = Document(url="doc1", pub_date=15, fetch_time=10, text="text3")
    input2 = Document(url='doc1', pub_date=15, fetch_time=20, text='text1', first_fetch_time=10)
    out_doc2 = processor.process(doc2, state)
    assert out_doc2 == input2

    doc3 = Document(url="doc1", pub_date=30, fetch_time=80, text="text4")
    input3 = Document(url='doc1', pub_date=15, fetch_time=80, text='text4', first_fetch_time=10)
    out_doc3 = processor.process(doc3, state)
    assert out_doc3 == input3
