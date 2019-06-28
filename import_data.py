import os
from neo4j import GraphDatabase
from mosr_back_orm.orm import create_session,SystemPar,init_db,SystemCode,ProcessDetail,SystemData,QueryTemplate,Neno4jCatalog,JobQueue,ImportData
import datetime
import uuid
from sqlalchemy import desc
import json
import ast
import csv
import sys
import platform
import os
sys.path.append("python_common")
sys.path.append("mosr_back_orm")

from python_common.database_common import Database




def import_data():
    db_session=create_session()
    #读取没有完结的queue
    queue=db_session.query(JobQueue).filter(JobQueue.u_status=='发布',JobQueue.u_declare_key=='import_data',JobQueue.u_publisher_id=='import_data').order_by(JobQueue.u_publish_datetime.desc()).first()
    job=queue
    if (queue!=None):
        #对应的import_data不是删除状态
        
        current=datetime.datetime.now()
           
            #解析body中的数据
        import_command=queue.u_body
        print(import_command)
        os.popen(import_command)
        #print(r_import_command)
        current=datetime.datetime.now()
            
        queue.u_status='处理完成'

        queue.u_complete_datetime=current
    db_session.commit()
    db_session.close()
    #处理下载数据
    #连接数据库
    

def main():

    import_data()

if __name__ == '__main__':
  main()