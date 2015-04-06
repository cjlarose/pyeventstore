def links_as_dict(links):
    return {link['relation']: link['uri'] for link in links}


class StreamEntry:
    def __init__(self, content):
        self._content = content
        self._links = None
        self.summary = content['summary']

    @property
    def links(self):
        if self._links is None:
            self._links = links_as_dict(self._content['links'])
        return self._links


class StreamPage:
    def __init__(self, content):
        self._content = content
        self._links = None

    @property
    def links(self):
        if self._links is None:
            self._links = links_as_dict(self._content['links'])
        return self._links

    def entries(self):
        for entry_content in reversed(self._content['entries']):
            yield StreamEntry(entry_content)
