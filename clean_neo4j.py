
#encoding=utf-8
import os
from mosr_back_orm.orm import create_session,SystemPar,init_db,SystemCode,ProcessDetail,SystemData,QueryTemplate,Neno4jCatalog,JobQueue,ImportData
import datetime
import platform
import time


def clean_neo4j():
    system_type=''
    if platform.platform().find('Windows')>=0:
        system_type='Windows'
    else:
        system_type='UNIX'
    db_session=create_session()
    queue=db_session.query(JobQueue).filter(JobQueue.u_status=='发布',JobQueue.u_declare_key=='clean_neo4j',JobQueue.u_publisher_id=='clean_neo4j').order_by(JobQueue.u_publish_datetime.desc()).first()
    
    job=queue
    if (queue!=None):
        current=datetime.datetime.now()
        queue.u_start_datetime=current
        db_session.commit()
        import_neo4j_install_dir=db_session.query(SystemPar).filter(SystemPar.par_code=='import_neo4j_install_dir').one()
        while True:
            if system_type=='Windows':
                windows_path=import_neo4j_install_dir.par_value.replace("/", "\\")
                if os.path.exists(windows_path+'data\\databases\\graph.db\\temp.db\\temp.db'):
                    del_db_command='del /q '+windows_path+'data\\databases\\graph.db\\temp.db\\temp.db'
                    r_del_db_command = os.popen(del_db_command).read()
                if os.path.exists(windows_path+'data\\databases\\graph.db\\temp.db'):
                    del_db_command='del /q '+windows_path+'data\\databases\\graph.db\\temp.db'
                    r_del_db_command = os.popen(del_db_command).read()
                if os.path.exists(windows_path+'data\\databases\\graph.db\\index'):
                    del_db_command='del /q '+windows_path+'data\\databases\\graph.db\\index'
                    r_del_db_command = os.popen(del_db_command).read()
                if os.path.exists(windows_path+'data\\databases\\graph.db\\profiles'):
                    del_db_command='del /q '+windows_path+'data\\databases\\graph.db\\profiles'
                    r_del_db_command = os.popen(del_db_command).read()
                if os.path.exists(windows_path+'data\\databases\\graph.db'):
                    del_db_command='del /q '+windows_path+'data\\databases\\graph.db'
                    r_del_db_command = os.popen(del_db_command).read()
                #删除目录
                if os.path.exists(windows_path+'data\\databases\\graph.db\\temp.db\\temp.db'):
                    del_db_command='rd '+windows_path+'data\\databases\\graph.db\\temp.db\\temp.db'
                    r_del_db_command = os.popen(del_db_command).read()
                if os.path.exists(windows_path+'data\\databases\\graph.db\\temp.db'):
                    del_db_command='rd '+windows_path+'data\\databases\\graph.db\\temp.db'
                    r_del_db_command = os.popen(del_db_command).read()
                if os.path.exists(windows_path+'data\\databases\\graph.db\\index'):
                    del_db_command='rd '+windows_path+'data\\databases\\graph.db\\index'
                    r_del_db_command = os.popen(del_db_command).read()
                if os.path.exists(windows_path+'data\\databases\\graph.db\\profiles'):
                    del_db_command='rd '+windows_path+'data\\databases\\graph.db\\profiles'
                    r_del_db_command = os.popen(del_db_command).read()
                if os.path.exists(windows_path+'data\\databases\\graph.db'):
                    del_db_command='rd '+windows_path+'data\\databases\\graph.db'
                    r_del_db_command = os.popen(del_db_command).read()
                #print("删除结束")
                #测试是否删掉
                if os.path.exists(windows_path+'data\\databases\\graph.db'):
                    #数据库并未停止继续删除
                    pass
                else:
                    #print("tingzhi")
                    break

            else:
                del_db_command='rm -Rf '+import_neo4j_install_dir.par_value+'data/databases/graph.db'
                r_del_db_command = os.popen(del_db_command).read()
                #测试是否删掉
                if os.path.exists(import_neo4j_install_dir.par_value+'data/databases/graph.db'):
                    #数据库并未停止继续删除
                    pass
                else:
                    break
            time.sleep(10)
        current=datetime.datetime.now()
        queue.u_complete_datetime=current
        queue.u_status='处理完成'
        db_session.commit()
        db_session.close()