
from dfilter import Dfilter


class TestCountries(object):

    """
    Do some queries on countries.json: Got this file from someone on github.
    I should go look from who and give them some credit. But, thanks so long.

    """
    def setup(self):
        self.df = Dfilter()
        self.df.read_json('tests/countries.json')

    def test_01_count(self):
        count = self.df.count()
        assert count == 250, "There was {0}".format(count)
        assert len(self.df) == count, "Len and count() do not match."

    def test_02_country(self):
        sa = self.df.filter({'*.name': 'South Africa'})
        assert 'South Africa' == sa.fetch('*.name'), \
               "Filter result must match request"
        sa_name = sa.fields('*.name').fetch('*.name')
        assert 'South Africa' == sa_name
#
