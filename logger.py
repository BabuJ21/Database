import logging as lp
lp.basicConfig(filename="database.log",level=lp.INFO, format='%(levelname)s %(asctime)s %(message)s')
lp.info("Logging is enabled")