# #!/usr/bin/env python

# import logging
# from logging.handlers import RotatingFileHandler
# from os import path, popen


# EB_LOGGING_DIRECTORY = '/opt/python/log/'
# PROJECT_NAME = 'kondoboard-etl'


# def get_log(filename):
# 	return (
# 		path.join(
# 			EB_LOGGING_DIRECTORY,
# 			f'{PROJECT_NAME}-{path.basename(filename)}.log',
# 		)
# 	)


# def tail_log(filename, n_lines=100):
# 	filepath = getLogFile(filename)
# 	return(popen(f'tail -n {int(n_lines)} {filepath}').read())


# def start_log(file):
# 	log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s:%(filename)s:%(funcName)s(%(lineno)d) %(message)s')

# 	if file is not None:
# 		my_handler = RotatingFileHandler(file, mode='a', maxBytes=8 * 1024 * 1024,
# 									backupCount=16, encoding=None, delay=0)
# 	else:
# 		my_handler = logging.StreamHandler()
# 	my_handler.setFormatter(log_formatter)
# 	my_handler.setLevel(logging.DEBUG)

# 	root_log = logging.getLogger()
# 	root_log.setLevel(logging.DEBUG)

# 	root_log.addHandler(my_handler)
# 	root_log.critical('********************************')
# 	root_log.critical('App started, logging initialized')
# 	return(root_log)
