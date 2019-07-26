
#encoding=utf-8
import os
from mosr_back_orm.orm import AlgorithmRsCCDD,AlgorithmRsCCD,AlgorithmRsCCM,create_session,SystemPar,init_db,SystemCode,ProcessDetail,SystemData,QueryTemplate,Neno4jCatalog,JobQueue,ImportData
import datetime
from python_common.neo4j_common import command
import time
import uuid
import datetime



def readData(command_sql,records):
        db_session=create_session()
        uuid_str=str(uuid.uuid1())
        
        setId=0
        setSize=0
        for record in records:
                #print(record)
                
                algorithmRsCCDD=AlgorithmRsCCDD(u_uuid=uuid_str,u_setId=setId,u_nodeId=record['nodeId'])
                db_session.add(algorithmRsCCDD)
                #print(record['setId'])
                if setId!=record['setId']:
                        #新的setId
                        if setId!="":
                                
                                algorithmRsCCD=AlgorithmRsCCD(u_uuid=uuid_str,u_setId=setId,u_size=setSize)
                                db_session.add(algorithmRsCCD)
                                setId=record['setId']
                                setSize=0
                else:
                        pass
                setSize+=1

        algorithmRsCCD=AlgorithmRsCCD(u_uuid=uuid_str,u_setId=setId,u_size=setSize)
        db_session.add(algorithmRsCCD)


        algorithmRsCCM=AlgorithmRsCCM(u_uuid=uuid_str,u_create_datetime=datetime.datetime.now(),u_queue_string=command_sql)
        db_session.add(algorithmRsCCM)

        db_session.commit()
        db_session.close()


def unionFind():
        db_session=create_session()
        queue=db_session.query(JobQueue).filter(JobQueue.u_status=='发布',JobQueue.u_declare_key=='algo.unionFind',JobQueue.u_publisher_id=='algo.unionFind').order_by(JobQueue.u_publish_datetime.desc()).first()
        job=queue
        if (queue!=None):
                current=datetime.datetime.now()
                queue.u_start_datetime=current
                db_session.commit()
                body=queue.u_body
                print(body)
                rs_uuid=command(body,readData)
                current=datetime.datetime.now()
                queue.u_complete_datetime=current
                queue.u_status='处理完成'
                queue.u_back_message=rs_uuid
                db_session.commit()
        db_session.close()
        #print("ok")