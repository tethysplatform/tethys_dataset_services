import os
import unittest
import xml.etree.ElementTree as ET
from tethys_dataset_services import utilities
from tethys_dataset_services.utilities import XmlDictObject


class TestUtilities(unittest.TestCase):
    def setUp(self):
        # Files
        self.tests_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.files_root = os.path.join(self.tests_root, 'files')

    def tearDown(self):
        pass

    def test_ConvertDictToXml(self):
        dict_data = {'note': {'importance': 'high', 'todo':
                              [{'type': 'active', '_text': 'Work'},
                               {'type': 'active', '_text': 'Play'},
                               {'type': 'active', '_text': 'Eat'},
                               {'type': 'passive', '_text': 'Sleep'}],
                              'logged': 'true', 'title': ['Happy', 'Happy']}}

        result = utilities.ConvertDictToXml(dict_data)
        xmlstr = ET.tostring(result)
        solution = '<note><importance>high</importance><logged>true</logged>' \
                   '<todo>Work<type>active</type></todo><todo>Play<type>active</type>' \
                   '</todo><todo>Eat<type>active</type></todo><todo>Sleep' \
                   '<type>passive</type></todo>' \
                   '<title>Happy</title><title>Happy</title></note>'

        # Check Result
        self.assertEqual(xmlstr, solution)

    def test_ConvertXmlToDict(self):
        file_name = 'test.xml'
        xml_file = os.path.join(self.files_root, file_name)

        dict_data = utilities.ConvertXmlToDict(root=xml_file)

        solution = {'note': {'importance': 'high',
                             'todo': [{'type': 'active', '_text': 'Work'},
                                      {'type': 'active', '_text': 'Play'},
                                      {'type': 'active', '_text': 'Eat'},
                                      {'type': 'passive', '_text': 'Sleep'}],
                             'logged': 'true', 'title': ['Happy', 'Happy']}}

        # Check Result
        self.assertEqual(dict_data, solution)

    def test_ConvertXmlToDict_TypeError(self):
        dictionary = {'1': '2'}
        self.assertRaises(TypeError, utilities.ConvertXmlToDict, root=dictionary)

    def test_XmlDictObject_dict(self):
        dict_data = {'to_do': 'work', 'list1': ['test1', 'test2']}

        result = XmlDictObject.Wrap(dict_data)

        # Check setattr
        self.assertEqual(str(result), '')
        result._text = 'Hello, world!'
        result.x = 10
        result.list2 = ['test3', 'test4']

        # Check new object
        self.assertEqual(str(result), 'Hello, world!')
        self.assertEqual(result.x, 10)
        self.assertEqual(result.to_do, 'work')
        self.assertEqual(result.list1, ['test1', 'test2'])
        self.assertEqual(result.list2, ['test3', 'test4'])

        # UnWrap
        new_dict = result.UnWrap()

        # Check new dict
        self.assertEqual(new_dict['_text'], 'Hello, world!')
        self.assertEqual(new_dict['x'], 10)
        self.assertEqual(new_dict['list1'], ['test1', 'test2'])
        self.assertEqual(new_dict['list2'], ['test3', 'test4'])
