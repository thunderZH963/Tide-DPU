import unittest
from run_recovery_bench import parse_run, FIELDS

class RecoveryParserTests(unittest.TestCase):
    def sample(self):
        return ','.join(FIELDS)+'\n'+''.join(f'{n},1,2,1,2,100\n' for n in [1,64,256,1024])
    def test_valid(self):
        self.assertEqual(len(parse_run(self.sample())),4)
    def test_invalid_evidence(self):
        for value in [self.sample().replace('64,','1,'), self.sample().replace(',1,2,',',nan,2,'),
                      self.sample().replace(',1,2,',',3,2,'), self.sample().replace(',100',',0'),
                      self.sample().split('\n',1)[1], '']:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):parse_run(value)

if __name__=='__main__':unittest.main()
