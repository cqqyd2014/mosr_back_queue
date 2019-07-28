from mosr_back_orm.orm import create_session,SystemPar,init_db,SystemCode,ProcessDetail,SystemData,QueryTemplate,Neno4jCatalog,JobQueue,ImportData
import datetime
import sys

def opendb_getjob(u_declare_key,job):
    sys.path.append("python_common")
    sys.path.append("mosr_back_orm")
    try:
        db_session=create_session()

        queue=db_session.query(JobQueue).filter(JobQueue.u_status=='发布',JobQueue.u_declare_key==u_declare_key,JobQueue.u_publisher_id==u_declare_key).order_by(JobQueue.u_publish_datetime.desc()).first()

        
        if (queue!=None):
            current=datetime.datetime.now()
            queue.u_start_datetime=current
            queue.u_status='处理中'
            db_session.commit()
            current=job(db_session,queue,current)
            current=datetime.datetime.now()
            queue.u_complete_datetime=current
            queue.u_status='处理完成'
            db_session.commit()
    except Exception as ex:
        db_session.rollback()
        print(ex)

    finally:
        db_session.close()