import configparser
import os
import sys
import logging
#from logging_setup import setup_logging

logger = logging.getLogger(__name__)

class iniReader:
    def __init__(self, filename="config.ini"):
        self.filename = filename
        self.logger = logger
        self.config = self.read_config_file()

    def read_config_file(self):
        config = configparser.ConfigParser()
        try:
            if not os.path.isfile(self.filename):
                raise FileNotFoundError(f"The file {self.filename} does not exist.")
            
            config.read(self.filename)
            
            if not config.sections():
                raise ValueError(f"The file {self.filename} is empty or cannot be parsed.")
                
            self.logger.info("Read config file successfully")
            return config
        except Exception as e:
            self.logger.error(f"Error reading config file: {e}")
            sys.exit(1)

    def validate(self):
        if 'global' in self.config:
            global_section = self.config['global']
            self.logger.log(logging.DEBUG, "[global] section:")
            for option in global_section:
                self.logger.log(logging.DEBUG, f"{option} = {global_section[option]}")
            self.logger.log(logging.DEBUG, f"\n")
            if 'database_type' in self.config['global']:
                db_type=self.config['global']['database_type']
            else:
                self.logger.log(logging.DEBUG, f"No database_type specified. defaulting to postgresql")
                db_type='postgresql'
        else:
            logging.error("Invalid config file: missing global section")
            sys.exit(1)

        # Check: 2
        if 'source' in self.config:
            source_section = self.config['source']
            self.logger.log(logging.DEBUG, "[source] section:")
            for option in source_section:
                if option != 'password':
                    self.logger.log(logging.DEBUG, f"{option} = {source_section[option]}")
            self.logger.log(logging.DEBUG, f"\n")
        else:
            logging.error("Invalid config file: missing source section")
            sys.exit(1)

        # Check config file : command is schema_backup. Check if schema_backup section is missing
        if self.config['global']['command'] == 'schema_backup':
            if 'schema_backup' in self.config:
                schema_backup_section = self.config['schema_backup']
                self.logger.log(logging.DEBUG, "[schema_backup] section:")
                for option in schema_backup_section:
                    self.logger.log(logging.DEBUG, f"{option} = {schema_backup_section[option]}")
                self.logger.log(logging.DEBUG, f"\n")
            else:
                logging.error("Invalid config file: missing schema_backup section")
                sys.exit(1)

        # Check config file : compare command
        if self.config['global']['command'] == 'compare':
            if 'source' in self.config and 'target' in self.config:
                logging.debug("source and target sections present")
            else:
                logging.error("Invalid config file: missing source or target section")
                sys.exit(1)

    def get_property(self, section, property_name, default_value):
        if section in self.config and property_name in self.config[section]:
            return self.config[section][property_name]
        else:
            return default_value
