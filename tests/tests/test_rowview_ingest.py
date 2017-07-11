import os, sys
PROJECT_HOME = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(PROJECT_HOME)
import unittest
from adsputils import load_config, setup_logging
from adsdata import reader

class test_rowview_ingest(unittest.TestCase):

    def setUp(self):
        self.config = {}
        self.config.update(load_config())


    def test_bibcode_reader(self):
        """verify bibcode reader adds id to every line"""
        filename = self.config['TEST_DATA_PATH'] + self.config['CANONICAL']
        lines_in_file = sum(1 for line in open(filename))
        r = reader.BibcodeFileReader(filename)
        bibcode_count = 0
        line = r.read()
        while line:
            bibcode_count += 1
            parts = line.split('\t')
            self.assertEqual(2, len(parts), 'bibcode lines should only include bibcode and integer')
            self.assertTrue(parts[1].strip().isdigit(), 'invalid value for id field: {}'.format(parts[1]))
            line = r.read()
        r.close()
        self.assertEqual(lines_in_file, bibcode_count, 'bibcode reader returned wrong number of lines')
    
    
    def test_refereed_reader(self):  
        """verify refereed reader adds True to every line"""
        filename = self.config['TEST_DATA_PATH'] + self.config['REFEREED']
        lines_in_file = sum(1 for line in open(filename))
        r = reader.RefereedFileReader(filename)
        bibcode_count = 0
        line = r.read()
        while line:
            bibcode_count += 1
            parts = line.split('\t')
            self.assertEqual(2, len(parts), 'refereed lines should only include bibcode and T')
            self.assertEqual('T', parts[1].strip(), 'invalid value for refereed field: {}'.format(parts[1]))
            line = r.read()
        r.close()
        self.assertEqual(lines_in_file, bibcode_count, 'refereed reader returned wrong number of lines')
    
    
    def test_author_reader(self):  
        """verify author reader creates the correct sql array value"""
        spot_checks = (('1057wjlf.book.....C', '{"Chao, C"}'),
                       ('1954PhRv...93..256R', '{"Rossi, G","Jones, W","Hollander, J","Hamilton, J"}'),
                       ('2017PDU....15...35R', '{"Raccanelli, A","Shiraishi, M","Bartolo, N","Bertacca, D","Liguori, M","Matarrese, S","Norris, R","Parkinson, D"}')
                       )
        self.standard_reader_test('author', spot_checks)

        
    def test_simbad_reader(self):  
        """verify simbad reader creates the correct sql array value"""
        
        spot_checks = (('2009A&A...503L..17C', '{"3068821 reg"}'),
                       ('2009ApJ...694.1228D', '{"1355682 *","746151 *"}'),
                       ('2016ApJ...822L..21L', '{"1423441 BLL","1535783 QSO"}'))
        self.standard_reader_test('simbad', spot_checks)
        
        
    def test_grants_reader(self):  
        """verify grants reader creates the correct sql array value"""
        
        spot_checks = (('2010AJ....140..933R', '{"NSF-AST 0706980"}'),
                       ('2010PhRvD..82b3004C', '{"NASA-HQ NNX09AC89G","NSF-AST 0807564"}'),
                       ('2012Icar..221.1162V', '{"NASA-HQ NNX11AD62G"}'))
        self.standard_reader_test('grants', spot_checks)
       
        
    def test_citation_reader(self):  
        """verify citation reader creates the correct sql array value"""
        
        spot_checks = ()
        self.standard_reader_test('citation', spot_checks)
       
        
    def test_relvance_reader(self):  
        """verify relevance reader creates the correct sql array value"""
        
        spot_checks = (('1057wjlf.book.....C', '0.32\t0\t25\t0\n'),
                       ('1992CRSSM..17..547W', '0.49\t153\t7\t10938\n'),
                       ('2017PDU....15...35R', '0.53\t0\t19\t0\n'))
        self.standard_reader_test('relevance', spot_checks)
        
    def test_reads_reader(self):  
        """verify reader reader creates the correct sql array value"""
        
        spot_checks = (('1057wjlf.book.....C', '{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,21,6}'),
                       ('2016A&A...594A..17P', '{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,293,232,37}'),
                       ('2017PDU....15...35R', '{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,19}'))
        self.standard_reader_test('reads', spot_checks)
        
        
    def test_download_reader(self):  
        """verify download reader creates the correct sql array value"""
        
        spot_checks = (('1057wjlf.book.....C', '{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}'),
                       ('2015MNRAS.447.2671Z', '{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,9,77,21,8}'),
                       ('2017PDU....15...35R', '{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5}'))
        self.standard_reader_test('download', spot_checks)
        
        
    def test_reference_reader(self):  
        """verify reference reader creates the correct sql array value"""
        
        spot_checks = (('1905PhRvI..21..247N', '{1904PhRvI..18..355N}'),
                       ('2015MNRAS.447.2671Z', '{1956ApJ...124...20S,1977ApJ...218..148M,1984ApJ...281...90H,1989ApJ...345..245C,1993ApJ...407...83S,1996ASPC...99..117G,1997ApJ...478...80H,1997MNRAS.286..513R,1998ApJS..114...73G,1999AJ....117.2594G,1999ApJ...516..750C,2000ApJ...544..581B,2000ApJ...545...63E,2001ApJ...557....2C,2001ApJ...561..684K,2001ASPC..247..175K,2002ApJ...574..643K,2002ApJ...577...98K,2002ASPC..255...69K,2003ApJ...583..178G,2003ApJ...595..120G,2003ApJ...598..232B,2003ARA&A..41..117C,2004ApJ...616..688P,2004ApJS..152....1S,2004IAUS..222..223K,2005A&A...434..569S,2005A&A...440..775K,2005ApJ...633..693K,2005ApJ...634..193S,2006ApJS..163..282W,2007ApJ...657..271C,2007ApJ...658..829A,2009ApJ...705.1320G,2009ApJS..182..378W,2009ASPC..411..301N,2010ApJ...721..960N,2010ApJ...724..318N,2010IAUS..267..201D,2010MNRAS.401....7H,2010SSRv..157..265C,2011A&A...534A..41K,2011A&A...534A..42S,2011Ap&SS.335..257O,2011ApJ...737..103S,2011ApJ...739..105S,2011MNRAS.410.2274Z,2012ApJ...746..125H,2012ApJ...746..173T,2012ApJ...752..162S,2012ApJ...753...75C,2012ASPC..460...83K,2013A&A...549A.100K,2013A&A...556A..94D,2013ApJ...776...99R,2013MNRAS.430.2650L,2013MNRAS.435.3028E}'),
                       )
        self.standard_reader_test('reference', spot_checks)
        
        
    def test_alsoread_reader(self):  
        """verify reference reader creates the correct sql array value"""
        
        spot_checks = (('1057wjlf.book.....C', '{4fc45951aa,557ebfd055,57fcb9018a}'),
                       ('2005cond.mat..7200L', '{Xa479ff933,Xf6ad5f036}')
                       )
        self.standard_reader_test('reader', spot_checks)
        
        
    def standard_reader_test(self, file_type, spot_checks):  
        """verify standard reader creates the correct sql value
        
        spot_checks is a list of (bibcode, value) pairs to verify.
        spot_checks might include first and last bibcodes in file 
        and other interesting or edge cases.
        """
        filename = self.config['TEST_DATA_PATH'] + self.config[file_type.upper()]
        lines_in_file = sum(1 for line in open(filename))
        r = reader.StandardFileReader(file_type, filename)
        bibcode_count = 0
        line = r.read()
        spot_checks_found = []
        multi_line = ('simbad', 'grants', 'citation', 'reference', 'reader')
        multi_value = ('relevance')
        while line:
            bibcode_count += 1            
            parts = line.split('\t')            
            bibcode = parts[0].strip()
            if file_type in multi_value:
                value = line[20:]
            else:
                value = parts[1].strip()            
            if file_type not in multi_value:
                self.assertEqual(2, len(parts), '{} lines should only include bibcode and value array {}'.format(file_type, line))
                self.assertEqual('{', value[0], 'invalid sql array {}'.format(value))
                self.assertEqual('}', value[-1], 'invalid sql array {}'.format(value))
            # we spot check a couple author fields
            for spot_check in spot_checks:
                spot_bibcode = spot_check[0]
                if bibcode == spot_bibcode:
                    spot_checks_found.append(spot_bibcode)
                    spot_value = spot_check[1]
                    self.assertEqual(spot_value, value, 
                                     'bad {} value for bibcode {}, expected {}, received {}'.format(file_type, bibcode, spot_value, value))
            line = r.read()
        r.close()
        self.assertEqual(len(spot_checks), len(spot_checks_found), 'for {} did not find all spot checks'.format(file_type))
        if file_type not in multi_line:
            self.assertEqual(lines_in_file, bibcode_count, 
                             '{} standard reader returned wrong number of lines'.format(file_type))    


if __name__ == '__main__':
    unittest.main(verbosity=2)        
        
