
import os
from dfilter import Dfilter

class TestFunctional(object):

    def setup(self):
        filename = "world_bank_countries.json"
        filename = "wbc.json"
        self.filename = filename
        if not os.path.isfile(self.filename):
            self.filename = os.path.join('tests', filename)
            if not os.path.isfile(self.filename):
                assert False, "could not find our data file."
        self.df = Dfilter()
        self.df.read_json(self.filename)

    def test_count(self):
        """Select an item from the dictionary."""
        assert self.df.count() == 50, "Count the countries in the file."

    def test_select(self):
        countries = self.df.select("12.region.id")
        print countries
        assert countries.count() == 1, 'Select One Country'
        countries = self.df.select("1[23].name")
        print countries
        assert countries.count() == 2, 'Select One Country'

    def test_find(self):
        query = {"*.region.id": u"NA"}
        countries = self.df.find(query)
        print countries.count()
        assert countries.count() == 100
