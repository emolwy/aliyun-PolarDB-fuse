# coding=utf-8

import os
from configparser import ConfigParser, NoOptionError

from components.singleton import Singleton


class Config(metaclass=Singleton):
    def __init__(self, config_file_path=None):
        self.config_parser = ConfigParser()
        self.config_file_path = config_file_path or os.path.join(
            os.path.dirname(__file__), '../app.config')
        self.load_config()

    def load_config(self):
        self.config_parser.read(self.config_file_path, 'utf-8')

    def get(self, key, default=None):
        '''
        获取配置
        ：param str key: 格式 [section].[key] 如：apps.NameError
        : param Any default: 默认值
        ：return:
        '''
        map_key = key.split('.')
        if len(map_key) < 2:
            return default
        section = map_key[0]
        if not self.config_parser.has_section(section):
            return default
        option = '.'.join(map_key[1:])
        try:
            return self.config_parser.get(section, option)
        except NoOptionError:
            return default


def get(key, default=None):
    return Config().get(key, default)
