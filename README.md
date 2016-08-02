# PyLog2MongoDB
dblog.py can save log to MongoDB directly. 

#Quick Start
from dblog import DBLogger
logger = DBLogger(database_name='bmclog')
logger.info("This is Info\n")
logger.error("This is Error\n")
logger.warn("This is Warning\n")

#Search the log
You need search the log by Mongodb, use 
