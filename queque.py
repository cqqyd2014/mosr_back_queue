

import os
from neo4j import GraphDatabase
from mosr_back_orm.orm import create_session,SystemPar,init_db,SystemCode,ProcessDetail,SystemData,QueryTemplate,Neno4jCatalog,JobQueue,ImportData
import datetime
import uuid
import time
from download_data import download_data
from import_data import import_data
from clean_neo4j import clean_neo4j
from neo4j_command import neo4j_command
from algoUnionFind import unionFind

import sys
sys.path.append("python_common")
sys.path.append("mosr_back_orm")


ps=0
try:
    db_session=create_session()
    #读取执行的间隔
    polling_second=db_session.query(SystemPar).filter(SystemPar.par_code=='polling_second').one()
    ps=int(polling_second.par_value)
    db_session.commit()
    
except:
    db_session.rollback()
    raise
finally:
    db_session.close()
print("连接数据库成功，轮询间隔"+str(ps))
if ps>0:
    while True:
        
        #
        download_data()
        import_data()
        clean_neo4j()
        neo4j_command()
        unionFind()
        time.sleep(ps)
