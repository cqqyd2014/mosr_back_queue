

import os
from neo4j import GraphDatabase
from mosr_back_orm.orm import create_session,SystemPar,init_db,SystemCode,ProcessDetail,SystemData,QueryTemplate,Neno4jCatalog,JobQueue,ImportData
import datetime
import uuid
import time
from download_data import download_data

import sys
sys.path.append("python_common")
sys.path.append("mosr_back_orm")


db_session=create_session()
#读取执行的间隔
polling_second=db_session.query(SystemPar).filter(SystemPar.par_code=='polling_second').one()
ps=polling_second.par_value
db_session.commit()
db_session.close()

ps=5
while True:
    
    #
    download_data()
    time.sleep(int(ps))
