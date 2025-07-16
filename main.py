# На вход сервису поступают обновления документов

# message Document {
#     string Url = 1;  // URL документа, его уникальный идентификатор
#     uint64 PubDate = 2;  // время заявляемой публикации документа
#     uint64 FetchTime = 3; // время получения данного обновления документа, может рассматриваться как идентификатор версии. Пара (Url, FetchTime) уникальна.
#     string Text = 4; // текст документа
#     uint64 FirstFetchTime = 5; // изначально отсутствует, необходимо заполнить
# }

from dataclasses import dataclass


@dataclass
class Document:
    url: str
    pub_date: int
    fetch_time: int
    text: str
    first_fetch_time: int = None



class TProcessor:

    def process(self, input_doc: Document, state: dict) -> Document:
        key = input_doc.url
        ft = input_doc.fetch_time
        s = state.get(key)

        if not s:
            state.update(
                {
                    key: {
                        "min_fetch": ft,
                        "min_pub_date": input_doc.pub_date,
                        "max_fetch": ft,
                        "max_text": input_doc.text,
                    }
                }
            )
            input_doc.first_fetch_time = ft
            return input_doc

        if ft < s["min_fetch"]:
            s["min_fetch"] = ft
            s["min_pub_date"] = input_doc.pub_date

        if ft > s["max_fetch"]:
            s["max_fetch"] = ft
            s["max_text"] = input_doc.text

        return Document(
            url=input_doc.url,
            pub_date=s["min_pub_date"],
            fetch_time=s["max_fetch"],
            text=s["max_text"],
            first_fetch_time=s["min_fetch"],
        )


if __name__ == "__main__":
    state = {}
    processor = TProcessor()

    documents = [
        Document(url="doc1", pub_date=10, fetch_time=20, text="text1"),
        # Document(url="doc2", pub_date=20, fetch_time=30, text="text2"),
        Document(url="doc1", pub_date=15, fetch_time=10, text="text3"),
        Document(url="doc1", pub_date=30, fetch_time=80, text="text4"),
    ]

    # out_doc = processor.process(documents[0])
    for d in documents:
        print(state)
        out_doc = processor.process(d, state)
        print(state)
        print(d, out_doc)


# Документы могут поступать в произвольном порядке (не в том, как они обновлялись), также возможно дублирование отдельных сообщений.

# Необходимо на выходе формировать такие же сообщения,
# но с исправленными отдельными полями по следующим правилам
# (всё нижеуказанное - для группы документов с совпадающим полем Url):

#     Поле Text и FetchTime должны быть такими, какими были в документе с наибольшим FetchTime, полученным на данный момент
#     Поле  PubDate должно быть таким,каким было у сообщения с наименьшим FetchTime
#     Поле FirstFetchTime должно быть равно минимальному значению FetchTime

# Т. е. в каждый момент времени мы берём PubDate и FirstFetchTime от самой первой из полученных на данный момент версий
# (если отсортировать их по FetchTime), а Text - от самой последней.

# Интерфейс в коде можно реализовать таким:
# class TProcessor {
#     public:
#         std::shared_ptr<TDocument> Process(std::shared_ptr<TDocument> input);
# };

# Для Go:
# type Processor interface {
#     Process(doc *Document) (*Document, error)
# }
# Данный код будет работать в сервисе, читающим входные сообщения из очереди сообщений (Kafka или подобное),
# и записывающем результат также в очередь. Если Process возвращает Null - то в очередь ничего не пишется.
